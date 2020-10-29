module.exports = cds.service.impl(srv => {
    srv.on("fetchCurrentWeatherData", _fetchCurrentWeatherData)
})

const fetch = require('node-fetch');
const { v4: uuidv4 } = require('uuid');
const moment = require('moment')

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

    var cities = await selectCities(conn_parms, conn);
    conn.disconnect();
}

async function selectCities(conn_parms, conn) {
    conn.connect(conn_parms);
    var result = conn.exec('SELECT * FROM "WEATHERDATA"."Relevant_Cities_for_API"');
    

    conn.exec("SET 'TIMESTAMP_FORMAT' = 'YYYY/MM/DD HH:MI:SS'")

    for (var i = 0; i < result.length; i++) {
        var id = uuidv4().toString();
        const weatherData = await getWeather(conn_parms, conn, result[i]);
        var currentData = conn.exec('SELECT * FROM "WEATHERBYPOSTALCODE" ORDER BY LASTUPDATE');
        console.log(currentData);
        //if (currentData.length === 0) {
            conn.exec(`INSERT INTO WEATHERBYPOSTALCODE VALUES('${id}', ${weatherData.postalCode}, ${weatherData.weatherConditionID}, ${weatherData.temperature}, '${weatherData.lastUpdate}')`, function (err, affectedRows) {
                if (err) console.error(err);
                console.log('Number of affected rows:', affectedRows);
            });
        //}
    }
    conn.disconnect();
    return result;
}

async function getWeather(conn_parms, conn, city) {
    var key = config.keys.crawler[0];
    console.log(city);
    const url = config.url.replace(/%s/g, encodeURI(`${city.Postalcode}`)).replace(/%c/g, encodeURI(`${city.Country}`))
        .replace(/%k/g, encodeURI(key));

    const response = await fetch(url);
    const json = await response.json();
    try {
        conn.exec('CREATE TABLE WEATHERBYPOSTALCODE(ID NVARCHAR(50), POSTALCODE NVARCHAR(10), WEATHERCONDITIONSID Integer, TEMPERATURE Double, LASTUPDATE Timestamp, PRIMARY KEY (ID))');
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
        console.error("Table already exists");
    }
    let postalCode = city.Postalcode;
    return {
        postalCode: postalCode.toString(),
        weatherConditionID: json.weather[0].id,
        temperature: json.main.temp,
        lastUpdate: moment(new Date(json.dt * 1000)).format("YYYY/MM/DD hh:mm:ss"),
    };
}