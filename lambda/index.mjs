import { DatabaseMigrationServiceClient, DescribeReplicationTasksCommand } from "@aws-sdk/client-database-migration-service";

export const handler = async (event) => {
  console.log("Received event:", JSON.stringify(event, null, 2));

  try {
    // Parse the table mappings
    const tableMappingsStr = event.tableMappings;

    if (!tableMappingsStr) {
      throw new Error("No tableMappings found in the event");
    }

    const tableMappings = JSON.parse(tableMappingsStr);

    // Extract table name
    const tableName = extractTableName(tableMappings);
    console.log(`Extracted table name: ${tableName}`);

    return {
      statusCode: 200,
      body: tableName
    };

  } catch (error) {
    console.error(`Error: ${error.message}`);
    return {
      statusCode: 500,
      error: error.message
    };
  }
};

function extractTableName(tableMappings) {
  if (!tableMappings.rules || tableMappings.rules.length === 0) {
    throw new Error("No rules found in the table mappings");
  }

  for (const rule of tableMappings.rules) {
    if (rule['rule-type'] === 'selection') {
      const objectLocator = rule['object-locator'] || {};
      const tableName = objectLocator['table-name'];

      if (!tableName) {
        throw new Error("Table name not found in the object locator");
      }

      return tableName; // Return only the table name, without the schema
    }
  }

  throw new Error("No selection rule found in the table mappings");
}
