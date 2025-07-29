const { Pool } = require("pg");
const dotenv = require("dotenv");

// Load environment variables
dotenv.config();

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT || 5432,
});

// Function to create the csvtojson table
async function createTable() {
  console.log("-------");
  console.log("Attempting to create or verify csvtojson table...");
  const query = `
    CREATE TABLE IF NOT EXISTS csvtojson (
      id SERIAL PRIMARY KEY,
      name VARCHAR NOT NULL,
      age INT NOT NULL,
      address JSONB,
      additional_info JSONB
    );
  `;
  try {
    await pool.query(query);
    console.log("Table csvtojson created or already exists");
  } catch (error) {
    console.error("Table creation failed:", error.message);
    throw new Error(`Failed to create table: ${error.message}`);
  }
}

// Function to insert data into csvtojson table
async function insertData(jsonData) {
  console.log("-------");
  console.log("Starting data insertion for", jsonData.length, "records...");
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    for (const record of jsonData) {
      const { name, age, address, ...additional_info } = record;
      const query = `
        INSERT INTO csvtojson (name, age, address, additional_info)
        VALUES ($1, $2, $3, $4)
      `;
      const values = [
        name,
        parseInt(age),
        address ? JSON.stringify(address) : null,
        Object.keys(additional_info).length > 0
          ? JSON.stringify(additional_info)
          : null,
      ];
      await client.query(query, values);
    }

    await client.query("COMMIT");
    console.log("Data insertion completed successfully");
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Data insertion failed:", error.message);
    throw new Error(`Failed to insert data: ${error.message}`);
  } finally {
    client.release();
    console.log("Database connection released");
  }
}

module.exports = { createTable, insertData };
