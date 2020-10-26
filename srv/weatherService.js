module.exports = cds.service.impl(srv => {
    srv.on("fetchCurrentWeatherData", _fetchCurrentWeatherData)
})

const fetch = require('node-fetch');

const config = {
    "url": "http://api.openweathermap.org/data/2.5/weather?q=%s,%c&appid=%k&units=metric",
    "keys": {
        "ondemand": "2360f5ae4148bdc6a326ea8e31faae0d",
        "crawler": [
            "2360f5ae4148bdc6a326ea8e31faae0d"
        ]
    },
    "limit": false,
    "timeout": 1000,
    "secret": "",
    "cleanOutdated": true,
    "keepLatest": 7
};

async function _fetchCurrentWeatherData(req) {
    const hana = require('@sap/hana-client');
    var conn = hana.createConnection();
    var conn_parms = {
        serverNode: "cc622444-1db2-4dd0-8cbe-71521705c697.hana.prod-eu10.hanacloud.ondemand.com:443",
        encrypt: true,
        uid: "WEATHERDATA#MASTER",
        pwd: "OKH(Ba(#aWYH9eA<",
    };

    var cities = selectCities(conn_parms, conn);

    getWeather(conn_parms, conn, cities);
}

function selectCities(conn_parms, conn) {
    conn.connect(conn_parms);
    var result = conn.exec('SELECT "Country","City","Postalcode" FROM "WEATHERDATA"."Relevant_Cities_for_API"');
    conn.disconnect();
    return result;
}

async function getWeather(conn_parms, conn, cities) {
    var key = config.keys.crawler[0];
    conn.connect(conn_parms);
    for (var i = 0; i < cities.length; i++) {
        const url = config.url.replace(/%s/g, encodeURI(`${cities[i].Postalcode}`)).replace(/%c/g, encodeURI(`${cities[i].Country}`))
            .replace(/%k/g, encodeURI(key));

        const response = await fetch(url);
        const json = await response.json();
        console.log(json);
        try {
            conn.exec('CREATE TABLE WEATHERBYPOSTALCODE(ID NVARCHAR, POSTALCODE NVARCHAR, WEATHERCONDITIONSID Integer, TEMPERATURE Double, LASTUPDATE Timestamp, PRIMARY KEY (ID))');
        } catch (error) {
            console.log("Table already exists");
        }

        try {
            conn.exec('CREATE TABLE WEATHERCONDITIONS(ID INTEGER, MAIN NVARCHAR, DESCRIPTION NVARCHAR, PRIMARY KEY (ID));');
        } catch (error) {
            console.log("Table already exists");
        }

        try {
            conn.exec('CREATE TABLE WEATHERCONDITIONS(ID INTEGER, MAIN NVARCHAR, DESCRIPTION NVARCHAR, PRIMARY KEY (ID));');
        } catch (error) {
            
        }
        
        console.log("Successfully inserted");
    }
    conn.disconnect()
}