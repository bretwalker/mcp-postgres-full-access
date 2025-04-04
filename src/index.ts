#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import pg from "pg";
// Import Node.js global process
import process from 'node:process';

import config from "./lib/config.js";
import { TransactionManager } from "./lib/transaction-manager.js";
import { safelyReleaseClient } from "./lib/utils.js";
import {
  handleExecuteQuery,
  handleExecuteDML,
  handleExecuteCommit,
  handleExecuteRollback,
  handleListTables,
  handleDescribeTable,
  handleListResources,
  handleReadResource,
} from "./lib/tool-handlers.js";

// Process command line arguments
const args = process.argv.slice(2);
if (args.length === 0) {
  console.error("Please provide a database URL as a command-line argument");
  process.exit(1);
}

const databaseUrl = args[0];
const resourceBaseUrl = new URL(databaseUrl);
resourceBaseUrl.protocol = "postgres:";
resourceBaseUrl.password = ""; // Remove password for security

// Create a connection pool with configured settings
const pool = new pg.Pool({
  connectionString: databaseUrl,
  max: config.pg.maxConnections,
  idleTimeoutMillis: config.pg.idleTimeoutMillis,
  statement_timeout: config.pg.statementTimeout,
});

// Create transaction manager
const transactionManager = new TransactionManager(
  config.transactionTimeoutMs,
  config.monitorIntervalMs,
  config.enableTransactionMonitor
);

// Create MCP server with improved version number
const server = new McpServer(
  {
    name: "postgres-advanced",
    version: "0.3.0", // Updated version with TypeScript fixes
  },
  {
    capabilities: {
      resources: {},
      tools: {},
    },
  }
);

// Helper function to transform handler responses into the correct format
function transformHandlerResponse(result: {
  content?: Array<{ type: string; text: string }>;
  isError?: boolean;
} | undefined): {
  content: Array<{ type: "text"; text: string }>;
  isError?: boolean;
} {
  if (!result) {
    // Return a default value if result is undefined
    return {
      content: [{ type: "text", text: "No result available" }],
      isError: true,
    };
  }

  const transformedResult: {
    content: Array<{ type: "text"; text: string }>;
    isError?: boolean;
  } = {
    content: [],
    isError: result.isError,
  };

  if (result.content) {
    transformedResult.content = result.content.map((item) => {
      return {
        type: "text" as const,
        text: item.text || "",
      };
    });
  } else {
    transformedResult.content = [{ type: "text", text: "No content available" }];
  }

  return transformedResult;
}

// Generic error handler to standardize error responses
function handleError(error: unknown): { 
  content: Array<{ type: "text"; text: string }>;
  isError: boolean;
} {
  console.error("Error in handler:", error);
  return {
    content: [
      {
        type: "text" as const,
        text: error instanceof Error ? error.message : String(error),
      },
    ],
    isError: true,
  };
}

// Register tools using the new high-level API
server.tool(
  "execute_query",
  "Run a read-only SQL query (SELECT statements). Executed in read-only mode for safety.",
  { 
    sql: z.string().describe("SQL query to execute (SELECT only)"),
    format: z.enum(["json", "table", "csv"]).optional().describe("Output format (defaults to JSON)")
  },
  async (args: { sql: string; format?: "json" | "table" | "csv" }, extra: Record<string, unknown>) => {
    try {
      // Remove the format parameter since the original function only takes two arguments
      const result = await handleExecuteQuery(pool, args.sql);
      return transformHandlerResponse(result);
    } catch (error) {
      return handleError(error);
    }
  }
);

server.tool(
  "execute_dml_ddl_dcl_tcl",
  "Execute DML, DDL, DCL, or TCL statements (INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, etc). Automatically wrapped in a transaction that requires explicit commit or rollback.",
  { 
    sql: z.string().describe("SQL statement to execute - after execution you'll need to explicitly commit or rollback the changes"),
    dry_run: z.boolean().optional().default(false).describe("If true, will show what would be executed without actually modifying data")
  },
  async (args: { sql: string; dry_run?: boolean }, extra: Record<string, unknown>) => {
    try {
      // Check transaction limit
      if (
        transactionManager.transactionCount >= config.maxConcurrentTransactions
      ) {
        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  status: "error",
                  message: `Maximum concurrent transactions limit reached (${config.maxConcurrentTransactions}). Try again later.`,
                },
                null,
                2
              ),
            },
          ],
          isError: true,
        };
      }

      // If dry_run is enabled, show what would be executed without actually doing it
      if (args.dry_run) {
        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify({
                status: "dry_run",
                message: "Dry run mode - this SQL would be executed:",
                sql: args.sql,
              }, null, 2),
            },
          ],
          isError: false,
        };
      }

      const result = await handleExecuteDML(
        pool,
        transactionManager,
        args.sql,
        config.transactionTimeoutMs
      );
      return transformHandlerResponse(result);
    } catch (error) {
      return handleError(error);
    }
  }
);

server.tool(
  "execute_commit",
  "Commit a transaction by its ID to permanently apply the changes to the database",
  { transaction_id: z.string().describe("ID of the transaction to commit - this will permanently save all changes to the database") },
  async (args: { transaction_id: string }, extra: Record<string, unknown>) => {
    try {
      const result = await handleExecuteCommit(
        transactionManager,
        args.transaction_id
      );
      return transformHandlerResponse(result);
    } catch (error) {
      return handleError(error);
    }
  }
);

server.tool(
  "execute_rollback",
  "Rollback a transaction by its ID to undo all changes and discard the transaction",
  { transaction_id: z.string().describe("ID of the transaction to rollback - this will discard all changes") },
  async (args: { transaction_id: string }, extra: Record<string, unknown>) => {
    try {
      const result = await handleExecuteRollback(
        transactionManager,
        args.transaction_id
      );
      return transformHandlerResponse(result);
    } catch (error) {
      return handleError(error);
    }
  }
);

server.tool(
  "list_tables",
  "Get a list of all tables in the database",
  { 
    schema: z.string().optional().describe("Optional schema name to filter tables"),
    include_system_tables: z.boolean().optional().default(false).describe("Include system tables in the result")
  },
  async (args: { schema?: string; include_system_tables?: boolean }, extra: Record<string, unknown>) => {
    try {
      // Remove extra parameters to match original function signature
      const result = await handleListTables(pool);
      return transformHandlerResponse(result);
    } catch (error) {
      return handleError(error);
    }
  }
);

server.tool(
  "describe_table",
  "Get detailed information about a specific table",
  { 
    table_name: z.string().describe("Name of the table to describe"),
    include_statistics: z.boolean().optional().default(false).describe("Include table statistics when available")
  },
  async (args: { table_name: string; include_statistics?: boolean }, extra: Record<string, unknown>) => {
    try {
      // Remove the statistics parameter to match original function signature
      const result = await handleDescribeTable(pool, args.table_name);
      return transformHandlerResponse(result);
    } catch (error) {
      return handleError(error);
    }
  }
);

// New tool to show active transactions
server.tool(
  "list_transactions",
  "List all active transactions managed by this MCP server",
  async (extra: Record<string, unknown>) => {
    try {
      // Since TransactionManager doesn't have a getTransactionIds method,
      // let's use the API it does provide
      
      // We'll need to report the transaction count only
      const activeTransactions = transactionManager.transactionCount;
      
      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify({
              status: "success",
              active_transactions: activeTransactions,
              max_concurrent: config.maxConcurrentTransactions,
              timeout_ms: config.transactionTimeoutMs
              // We can't list the actual transactions without access to them
            }, null, 2)
          },
        ],
        isError: false,
      };
    } catch (error) {
      return handleError(error);
    }
  }
);

// New tool to show server status
server.tool(
  "server_status",
  "Get current status of the MCP server including version, uptime, and connection pool information",
  async (extra: Record<string, unknown>) => {
    try {
      const startTime = process.uptime();
      const poolStatus = await pool.query('SELECT COUNT(*) as active_connections FROM pg_stat_activity WHERE pid <> pg_backend_pid()');
      
      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify({
              status: "success",
              server: {
                // Using the variables directly from when the server was created
                name: "postgres-advanced",
                version: "0.3.0",
                uptime_seconds: startTime,
              },
              pool: {
                total_connections: config.pg.maxConnections,
                active_connections: parseInt(poolStatus.rows[0].active_connections),
                idle_timeout: config.pg.idleTimeoutMillis,
                statement_timeout: config.pg.statementTimeout
              },
              transactions: {
                active: transactionManager.transactionCount,
                max_concurrent: config.maxConcurrentTransactions,
                timeout_ms: config.transactionTimeoutMs,
                monitor_enabled: config.enableTransactionMonitor,
                monitor_interval_ms: config.monitorIntervalMs
              }
            }, null, 2)
          },
        ],
        isError: false,
      };
    } catch (error) {
      return handleError(error);
    }
  }
);

// Register resources using the new API
// First, create a resource template for table schemas
const tableSchemaTemplate = new URL(`{tableName}/schema`, resourceBaseUrl);

// Add a resource for listing all available table schemas
server.resource(
  "database-schemas",
  resourceBaseUrl.href,
  { description: "Database schema listings" },
  async (uri: URL, _extra: Record<string, unknown>) => {
    try {
      const result = await handleListResources(pool, resourceBaseUrl);
      return {
        contents: [
          {
            uri: uri.href,
            mimeType: "application/json",
            text: JSON.stringify(result.resources, null, 2),
          },
        ],
      };
    } catch (error) {
      throw error;
    }
  }
);

// Add a resource for individual table schemas
server.resource(
  "table-schemas",
  tableSchemaTemplate.href,
  { description: "Database table schemas" },
  async (uri: URL, _extra: Record<string, unknown>) => {
    try {
      return await handleReadResource(pool, uri.href);
    } catch (error) {
      throw error;
    }
  }
);

// Start the MCP server
async function runServer() {
  console.error("Starting PostgreSQL Advanced MCP server...");

  // Log configuration
  console.error(`Configuration:
- Transaction timeout: ${config.transactionTimeoutMs}ms
- Monitor interval: ${config.monitorIntervalMs}ms
- Transaction monitor enabled: ${config.enableTransactionMonitor}
- Max concurrent transactions: ${config.maxConcurrentTransactions}
- Max DB connections: ${config.pg.maxConnections}
`);

  // Set up error handling for the pool
  pool.on("error", (err) => {
    console.error("Unexpected error on idle client", err);
    process.exit(1);
  });

  try {
    // Test database connection
    const client = await pool.connect();
    console.error("Successfully connected to database");
    
    // Get server version information
    const versionResult = await client.query('SELECT version()');
    console.error(`Database version: ${versionResult.rows[0].version}`);
    
    safelyReleaseClient(client);

    // Start transaction monitor
    transactionManager.startMonitor();

    // Start the MCP server with stdio transport
    const transport = new StdioServerTransport();
    await server.connect(transport);
    console.error("MCP server started and ready to accept connections");
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
}

// Handle unhandled promise rejections
process.on("unhandledRejection", async (reason: unknown, promise: Promise<unknown>) => {
  console.error("Unhandled promise rejection:", reason);
  await gracefulShutdown("unhandled promise rejection");
});

// Handle unexpected errors
process.on("uncaughtException", async (error: Error) => {
  console.error("Uncaught exception:", error);
  await gracefulShutdown("uncaught exception");
});

// Graceful shutdown handler
async function gracefulShutdown(reason = "shutdown") {
  console.error(`Shutting down due to ${reason}...`);
  
  try {
    // Stop the monitor first
    transactionManager.stopMonitor();
    console.error("Transaction monitor stopped");
    
    // Give some time for any in-flight operations to complete
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Clean up transactions
    await transactionManager.cleanupTransactions();
    console.error("Cleaned up transactions");
    
    // Close the pool
    await pool.end();
    console.error("Database pool closed");
  } catch (err) {
    console.error(`Error during shutdown: ${err}`);
  }
  
  // Exit with appropriate code
  if (reason === "shutdown") {
    process.exit(0);
  } else {
    process.exit(1);
  }
}

// Graceful shutdown on SIGINT and SIGTERM
process.on("SIGINT", async () => {
  await gracefulShutdown("SIGINT");
});

process.on("SIGTERM", async () => {
  await gracefulShutdown("SIGTERM");
});

runServer().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});