// queries.js
// MongoDB Week 1 Assignment Solutions

const { MongoClient } = require("mongodb");

async function runQueries() {
  const uri = "mongodb://127.0.0.1:27017"; // Change if using Atlas
  const client = new MongoClient(uri);

  try {
    await client.connect();
    const db = client.db("plp_bookstore");
    const books = db.collection("books");

    console.log("\n===== TASK 2: BASIC CRUD OPERATIONS =====");

    // 1. Find all books in a specific genre
    console.log("Books in genre 'Fiction':");
    console.log(await books.find({ genre: "Fiction" }).toArray());

    // 2. Find books published after a certain year
    console.log("Books published after 2015:");
    console.log(await books.find({ published_year: { $gt: 2015 } }).toArray());

    // 3. Find books by a specific author
    console.log("Books by 'John Doe':");
    console.log(await books.find({ author: "John Doe" }).toArray());

    // 4. Update the price of a specific book
    console.log("Updating price of 'The Great Book'...");
    await books.updateOne({ title: "The Great Book" }, { $set: { price: 19.99 } });

    // 5. Delete a book by its title
    console.log("Deleting book titled 'Old Book'...");
    await books.deleteOne({ title: "Old Book" });

    console.log("\n===== TASK 3: ADVANCED QUERIES =====");

    // 1. Books in stock AND published after 2010
    console.log("Books in stock & published after 2010:");
    console.log(
      await books.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray()
    );

    // 2. Projection (title, author, price)
    console.log("Projection (title, author, price):");
    console.log(
      await books
        .find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } })
        .toArray()
    );

    // 3. Sorting by price
    console.log("Books sorted by price ascending:");
    console.log(await books.find().sort({ price: 1 }).toArray());

    console.log("Books sorted by price descending:");
    console.log(await books.find().sort({ price: -1 }).toArray());

    // 4. Pagination: limit & skip (5 per page)
    const page = 1; // Change to 2, 3, etc.
    const pageSize = 5;
    console.log(`Books on page ${page}:`);
    console.log(await books.find().skip((page - 1) * pageSize).limit(pageSize).toArray());

    console.log("\n===== TASK 4: AGGREGATION PIPELINES =====");

    // 1. Average price by genre
    console.log("Average price by genre:");
    console.log(
      await books
        .aggregate([
          { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }
        ])
        .toArray()
    );

    // 2. Author with the most books
    console.log("Author with the most books:");
    console.log(
      await books
        .aggregate([
          { $group: { _id: "$author", count: { $sum: 1 } } },
          { $sort: { count: -1 } },
          { $limit: 1 }
        ])
        .toArray()
    );

    // 3. Group by decade
    console.log("Books grouped by publication decade:");
    console.log(
      await books
        .aggregate([
          {
            $group: {
              _id: { $subtract: [{ $divide: ["$published_year", 10] }, { $mod: [{ $divide: ["$published_year", 10] }, 1] }] },
              count: { $sum: 1 }
            }
          },
          { $sort: { _id: 1 } }
        ])
        .toArray()
    );

    console.log("\n===== TASK 5: INDEXING =====");

    // 1. Index on title
    console.log("Creating index on 'title'...");
    await books.createIndex({ title: 1 });

    // 2. Compound index on author + published_year
    console.log("Creating compound index on author + published_year...");
    await books.createIndex({ author: 1, published_year: -1 });

    // 3. Explain query performance
    console.log("Explain index usage for title search:");
    console.log(
      await books
        .find({ title: "The Great Book" })
        .explain("executionStats")
    );

  } finally {
    await client.close();
  }
}

runQueries();
