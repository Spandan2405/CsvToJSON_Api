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

// Function to calculate and print age distribution
async function calculateAgeDistribution() {
  console.log("-------");
  console.log("Calculating age distribution...");
  try {
    const result = await pool.query("SELECT age FROM public_users");
    const ages = result.rows.map((row) => parseInt(row.age) || 0); // Handle invalid ages
    const totalUsers = ages.length;

    if (totalUsers === 0) {
      console.log("No users found in the database.");
      return;
    }

    const ageGroups = {
      "< 20": 0,
      "20 to 40": 0,
      "40 to 60": 0,
      "> 60": 0,
    };

    ages.forEach((age) => {
      if (age < 20) ageGroups["< 20"]++;
      else if (age >= 20 && age <= 40) ageGroups["20 to 40"]++;
      else if (age > 40 && age <= 60) ageGroups["40 to 60"]++;
      else if (age > 60) ageGroups["> 60"]++;
    });

    console.log("Age-Group\t% Distribution");
    console.log("--------------------------------");
    for (const [group, count] of Object.entries(ageGroups)) {
      const percentage = ((count / totalUsers) * 100).toFixed(2);
      console.log(`${group}\t\t${percentage}`);
    }
  } catch (error) {
    console.error("Age distribution calculation failed:", error.message);
  }
}

module.exports = { calculateAgeDistribution };
