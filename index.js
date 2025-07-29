const express = require("express");
const { createTable, insertData } = require("./dbOperations");
const { calculateAgeDistribution } = require("./ageDistribution");
const fs = require("fs").promises;

const app = express();
const port = process.env.PORT || 3000;

// Function to parse CSV file into JSON (unchanged)
async function parseCsvToJson(filePath) {
  try {
    const data = await fs.readFile(filePath, "utf8");
    const lines = data.trim().split("\n");
    const headers = lines[0].split(",").map((header) => header.trim());
    const result = [];
    // console.log("headers", headers);

    for (let i = 1; i < lines.length; i++) {
      const values = lines[i].split(",").map((value) => value.trim());
      if (values.length !== headers.length) {
        console.warn(`Skipping malformed line ${i + 1}`);
        continue;
      }
      // console.log("values", values);
      const obj = {};
      for (let j = 0; j < headers.length; j++) {
        const header = headers[j];
        const value = values[j];

        if (header.includes(".")) {
          const [parent, child] = header.split(".");
          if (!obj[parent]) obj[parent] = {};
          obj[parent][child] = value;
        } else {
          obj[header] = value;
        }
      }
      // console.log(obj);

      if (obj.name.firstName && obj.name.lastName) {
        obj.name = `${obj.name.firstName} ${obj.name.lastName}`;
      }
      result.push(obj);
    }

    return result;
  } catch (error) {
    throw new Error(`Failed to parse CSV: ${error.message}`);
  }
}

// Express API endpoint to process CSV and store in DB
app.get("/process-csv", async (req, res) => {
  try {
    const filePath = process.env.CSV_FILE_PATH;
    if (!filePath) {
      throw new Error("CSV file path not configured");
    }
    console.log("-------");
    // Parse CSV to JSON
    const jsonData = await parseCsvToJson(filePath);

    // Create table if not exists
    await createTable();

    // Insert data into database
    await insertData(jsonData);

    // Calculate and print age distribution
    await calculateAgeDistribution();

    res.status(200).json({
      message: "CSV processed and data stored successfully",
      data: jsonData,
    });
  } catch (error) {
    console.error(error);
    res
      .status(500)
      .json({ error: "Internal server error", details: error.message });
  }
});

// Start the server
app.listen(port, async () => {
  console.log(`Server running on port ${port}`);
  try {
    await createTable();
  } catch (error) {
    console.error("Failed to initialize database:", error.message);
    process.exit(1);
  }
});
