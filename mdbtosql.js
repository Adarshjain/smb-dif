import fs from "fs";

import MDBReader from "mdb-reader";
// Path to your MDB file
const filePath = './pawn.mdb';
const tableName = 'itemdes'; // the table you want to export

// Load MDB file into buffer
const buffer = fs.readFileSync(filePath);
const reader = new MDBReader(buffer);

// Get the table
const table = reader.getTable(tableName);
if (!table) {
    console.error(`Table "${tableName}" not found.`);
    process.exit(1);
}

// Extract column info
const columns = table.getColumns();

// Map MDB types to PostgreSQL types
function mapType(column) {
    switch (column.type) {
        case 'Text':
            return `VARCHAR(${column.length || 255})`;
        case 'LongInteger':
            return 'INTEGER';
        case 'Double':
            return 'DOUBLE PRECISION';
        case 'Boolean':
            return 'BOOLEAN';
        case 'DateTime':
            return 'TIMESTAMP';
        case 'Currency':
            return 'MONEY';
        case 'Memo':
            return 'TEXT';
        case 'Byte':
            return 'SMALLINT';
        case 'Integer':
            return 'SMALLINT';
        case 'Guid':
            return 'UUID';
        case 'Binary':
            return 'BYTEA';
        default:
            return 'TEXT'; // fallback
    }
}

// Build CREATE TABLE statement
const columnDefs = columns.map((col) => {
    const pgType = mapType(col);
    const nullable = col.allowNull ? '' : ' NOT NULL';
    return `"${col.name}" ${pgType}${nullable}`;
});

const createTableSQL = `CREATE TABLE "${tableName}" (\n  ${columnDefs.join(',\n  ')}\n);`;

console.log(createTableSQL);
