const express = require('express');
const app = express();
const http = require('http');
require('dotenv').config();
const axios = require('axios');
const fs = require("fs");
const { stringify } = require("csv-stringify");
const limitExport = 500;
let mysql = require('mysql');
const util = require('util');
const { API_KEY, MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, SHOP, PASS } = process.env;

/* Config database*/
let connection = mysql.createConnection({
    host: MYSQL_HOST,
    user: MYSQL_USER,
    password: MYSQL_PASSWORD,
    database: MYSQL_DATABASE
});
const query = util.promisify(connection.query).bind(connection);
connection.connect(function (err) {
    if (err) {
        return console.error('error: ' + err.message);
    }
    console.info('Connected to the MySQL server.');
    // create customers table
    const sql = `CREATE TABLE IF NOT EXISTS customers (customer_id VARCHAR(25) NOT NULL, PRIMARY KEY (customer_id));`;
    connection.query(sql, function (err, result) {
        if (err) throw err;
        console.log("Create table customers success");
    });
});

app.get('/customers', async (http_request, http_response) => {
    try {
        const url = `https://${API_KEY}:${PASS}@${SHOP}/admin/api/2022-04/customers.json`

        const response = await axios.get(url,
            {
                headers: {
                    'content-type': 'application/json'
                }
            }
        );
        if (response && response.data && response.data.customers) {
            const customers = response.data.customers;
            const arrCustomers = []
            customers.length > 0 && customers.forEach(customer => {
                arrCustomers.push(customer.id.toString())
            });
            console.log('arrCustomers', arrCustomers);
            let dataInsert = ``;

            // export csv 
            let customersExportCsv = arrCustomers.slice(0, limitExport - 1);
            const filename = "customer.csv";
            const writableStream = fs.createWriteStream(filename);
            const columns = ["customer_id"];
            const stringifier = stringify({ header: true, columns: columns });

            for (let value of customersExportCsv) {
                stringifier.write([value]);
            }
            stringifier.pipe(writableStream);
            console.log("Finished writing data");

            // Insert data to mysql
            (async () => {
                try {
                    // Compare two Arrays(data get shopify and Databasse)and remove Duplicates
                    let arrCurrrents = await query(`SELECT customer_id FROM customers`);
                    arrCurrrents = arrCurrrents.map(item => item.customer_id)
                    const arrayInsert = arrCustomers.filter(val => !arrCurrrents.includes(val));

                    for (let value of arrayInsert) {
                        dataInsert = dataInsert ? dataInsert + `,(${value})` : `(${value})`
                    }

                    // insert data to Database
                    const rows = await query(`INSERT INTO customers VALUES${dataInsert};`);
                } finally {
                    connection.end();
                }
            })()
            http_response.send(`<html><body><p>Customer list </p></body></html>`);
        } else {
            http_response.send(`<html><body><p>Customer Empty list </p></body></html>`);

        }
    } catch (err) {
        console.log('error ', err);
    }
});

const httpServer = http.createServer(app);

httpServer.listen(3000, () => console.log('Your app is listening on port 3000.'));