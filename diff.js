import MDBReader from "mdb-reader";
import fs from "fs";
import {createClient} from "@supabase/supabase-js";
import dotenv from "dotenv";
dotenv.config();

// Load the .mdb file
const buffer = fs.readFileSync(process.env.PAWN_PATH);
const reader = new MDBReader(buffer);

// Extract tables
const billingTable = reader.getTable('billing');
const customerMasterTable = reader.getTable('customermaster');
const itemdesTable = reader.getTable('itemdes');

// Get data
let billingData = billingTable.getData();
billingData = billingData.filter(record => new Date(record.date) >= new Date('2020-01-01'));
const customerMasterData = customerMasterTable.getData();
const itemdesData = itemdesTable.getData();

// Initialize Supabase
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

const whiteListedColumns = [
    "serial",
    "nos",
    "date",
    "code",
    "name",
    "fhtitle",
    "fhname",
    "add1",
    "add2",
    "area",
    "loan",
    "items",
    "status",
    "redate",
    "des",
    "refer", //phone
    "intrate"
];

const lowerCaseKeysWhitelist = (obj) => {
    const keys = Object.keys(obj);
    const map = {};
    keys.forEach((key) => {
        const lowKey = key.toLowerCase();
        if (whiteListedColumns.includes(lowKey)) {
            map[lowKey] = obj[key];
        }
    });
    return map;
}
const lowerCaseKeys = (obj) => {
    const keys = Object.keys(obj);
    const map = {};
    keys.forEach((key) => {
        map[key.toLowerCase()] = obj[key];
    });
    return map;
}

// Function to sync billing table
async function syncBilling() {
    // Fetch existing records from Supabase
    const {data: supabaseBillingData, error} = await supabase
        .from('billing')
        .select('serial, nos, status');

    if (error) {
        console.error('Error fetching billing data from Supabase:', error);
        return;
    }

    // Create a map of existing records for quick lookup
    const supabaseBillingMap = new Map(
        supabaseBillingData.map((record) => [`${record.serial}-${record.nos}`, record])
    );

    // Arrays to hold new records and records to update
    const newRecords = [];
    const recordsToUpdate = [];

    // Iterate over local billing data
    for (const localRecord of billingData) {
        const key = `${localRecord.serial}-${localRecord.nos}`;
        const supabaseRecord = supabaseBillingMap.get(key);

        if (supabaseRecord) {
            // Check if STATUS has changed
            if (supabaseRecord.status !== localRecord.STATUS) {
                recordsToUpdate.push(localRecord);
            }
        } else {
            // New record
            newRecords.push(lowerCaseKeysWhitelist(localRecord));
        }
    }
    console.log('Loans:', newRecords.length + '.', 'Releases:', recordsToUpdate.length + '.')
    // Insert new records
    if (newRecords.length > 0) {
        const {error: insertError, count, statusText} = await supabase
            .from('billing')
            .insert(newRecords);

        if (insertError) {
            console.error('Error inserting new billing records:', insertError);
        } else {
            console.log(`Inserted ${newRecords.length} new billing records.`);
        }
    }


    // Update existing records
    if (recordsToUpdate.length > 0) {
        const updates = recordsToUpdate.map(record => ({
            serial: record.serial,
            nos: record.nos,
            status: record.STATUS,
            redate: record.redate,
        }));

        const {error: updateError} = await supabase
            .from('billing')
            .upsert(updates, {onConflict: ['serial', 'nos']});

        if (error) {
            console.error("Error updating billing records:", error);
        } else {
            console.log("Billing records updated successfully.");
        }
    }
}

// Function to sync itemdes table
async function syncItemDes() {
    // Fetch existing records from Supabase
    const {data: supabaseItemdesData, error} = await supabase
        .from('itemdes')
        .select('serial, nos, status');

    if (error) {
        console.error('Error fetching itemdes data from Supabase:', error);
        return;
    }

    // Create a map of existing records for quick lookup
    const supabaseItemdesMap = new Map(
        supabaseItemdesData.map((record) => [`${record.serial}-${record.nos}`, record])
    );

    // Arrays to hold new records and records to update
    const newRecords = [];
    const recordsToUpdate = [];

    // Iterate over local billing data
    for (const localRecord of itemdesData) {
        const key = `${localRecord.serial}-${localRecord.nos}`;
        const supabaseRecord = supabaseItemdesMap.get(key);

        if (supabaseRecord) {
            // Check if STATUS has changed
            if (supabaseRecord.status !== localRecord.STATUS) {
                recordsToUpdate.push(localRecord);
            }
        } else {
            // New record
            newRecords.push(lowerCaseKeys(localRecord));
        }
    }
    console.log('Item des', 'New Record:', newRecords.length + '.', 'Updates:', recordsToUpdate.length + '.')
    // Insert new records
    if (newRecords.length > 0) {
        const {error: insertError, count, statusText} = await supabase
            .from('itemdes')
            .insert(newRecords);

        if (insertError) {
            console.error('Error inserting new itemdes records:', insertError);
        } else {
            console.log(`Inserted ${newRecords.length} new itemdes records.`);
        }
    }


    // Update existing records
    if (recordsToUpdate.length > 0) {
        const updates = recordsToUpdate.map(record => ({
            serial: record.serial,
            nos: record.nos,
            status: record.STATUS,
            redate: record.redate,
        }));

        const {error: updateError} = await supabase
            .from('itemdes')
            .upsert(updates, {onConflict: ['serial', 'nos']});

        if (error) {
            console.error("Error updating itemdes records:", error);
        } else {
            console.log("Itemdes records updated successfully.");
        }
    }
}

// Function to sync customermaster table
async function syncCustomerMaster() {
    // Fetch existing records from Supabase
    const {data: supabaseCustomerData, error} = await supabase
        .from('customermaster')
        .select('code');

    if (error) {
        console.error('Error fetching customer data from Supabase:', error);
        return;
    }

    // Create a map of existing records for quick lookup
    const supabaseCustomerMap = new Map(
        supabaseCustomerData.map((record) => [record.code, record])
    );

    // Arrays to hold new records and records to update
    const newRecords = [];

    // Iterate over local customer data
    for (const localRecord of customerMasterData) {
        const supabaseRecord = supabaseCustomerMap.get(localRecord.code);

        if (!supabaseRecord) {
            newRecords.push(lowerCaseKeys(localRecord));
        }
    }
    console.log('New Customers: ', newRecords.length)
    // Insert new records
    if (newRecords.length > 0) {
        const {error: insertError} = await supabase
            .from('customermaster')
            .insert(newRecords);

        if (insertError) {
            console.error('Error inserting new customer records:', insertError);
        } else {
            console.log(`Inserted ${newRecords.length} new customer records.`);
        }
    }
}

// Execute the sync functions
(async () => {
    await syncBilling();
    await syncItemDes();
    await syncCustomerMaster();
})();
