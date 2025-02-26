import MDBReader from "mdb-reader";
import fs from "fs";
import {createClient} from "@supabase/supabase-js";


// Load the .mdb file
const buffer = fs.readFileSync('./pawn.mdb');
const reader = new MDBReader(buffer);

// Extract tables
const billingTable = reader.getTable('billing');
const customerMasterTable = reader.getTable('customermaster');

// Get data
let billingData = billingTable.getData();
billingData = billingData.filter(record => new Date(record.date) >= new Date('2020-01-01'));
const customerMasterData = customerMasterTable.getData();

// Initialize Supabase
const supabaseUrl = 'https://elckcrvxdqtyklgegswc.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVsY2tjcnZ4ZHF0eWtsZ2Vnc3djIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDAzODgyOTYsImV4cCI6MjA1NTk2NDI5Nn0.USK-VAxkSDwQxMTHYXixTwZaLiHrSwua_NWRlxcYROk';
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

const lowerCaseKeys = (obj) => {
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
            newRecords.push(lowerCaseKeys(localRecord));
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
            newRecords.push(localRecord);
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
    await syncCustomerMaster();
})();
