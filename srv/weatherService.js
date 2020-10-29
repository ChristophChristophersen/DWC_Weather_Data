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

    //get relevant cities
    var cities = await selectCities(conn_parms, conn);

    /*conn.exec("INSERT INTO WEATHERDATA.WEATHERBYPOSTALCODE VALUES('2d76de7e-af14-4371-abcf-65ae0b0dcce2', 502, 11, '2020-10-29T14:55:16.000Z')", function (err, affectedRows) {
              if (err) throw err;
              console.log('Number of affected rows:', affectedRows);
          });

    //get weather data for city and insert in the DWC
    for (var i = 0; i < cities.length; i++) {
        var city = cities[i];
        const weatherData = await getWeather(conn_parms, conn, city);
        //var currentData = conn.exec('SELECT * FROM "WEATHERDATA"."WEATHERBYPOSTALCODE" ORDER BY LASTUPDATE');
        //console.log(currentData);
        console.log(city);
        console.log(weatherData);
        //conn.connect();
        //var test = "INSERT INTO WEATHERBYPOSTALCODE VALUES('" + uuidv4() + "', " + weatherData.weatherConditionID + ", " + weatherData.temperature + ", '" + weatherData.lastUpdate + "')"
        /*if (currentData.length === 0) {
            conn.exec("INSERT INTO WEATHERBYPOSTALCODE VALUES('" + uuidv4() + " )", function (err, affectedRows) {
                if (err) throw err;
                console.log('Number of affected rows:', affectedRows);
            });
        }
        //console.log(test);

    }*/
    conn.disconnect();
}

async function selectCities(conn_parms, conn) {
    conn.connect(conn_parms);
    var result = conn.exec('SELECT "Country","City","Postalcode" FROM "WEATHERDATA"."Relevant_Cities_for_API"');
    var currentData = conn.exec('SELECT * FROM "WEATHERBYPOSTALCODE" ORDER BY LASTUPDATE');
    conn.exec("SET 'TIMESTAMP_FORMAT' = 'YYYY/MM/DD HH:MI:SS'")
    const weatherData = await getWeather(conn_parms, conn, result[0]);
    console.log(weatherData);
    conn.exec(`INSERT INTO WEATHERBYPOSTALCODE VALUES('${uuidv4().toString()}', ${weatherData.postalCode}, ${weatherData.weatherConditionID}, ${weatherData.temperature}, '${weatherData.lastUpdate}')`, function (err, affectedRows) {
        if (err) console.error(err);
        console.log('Number of affected rows:', affectedRows);
    });
    console.log(currentData);
    conn.disconnect();
    return result;
}

async function getWeather(conn_parms, conn, city) {
    var key = config.keys.crawler[0];

    //conn.connect(conn_parms);
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
    console.log(moment(new Date(json.dt * 1000)).format("DD/MM/YYYY hh:mm:ss"))
    return {
        postalCode: postalCode.toString(),
        weatherConditionID: json.weather[0].id,
        temperature: json.main.temp,
        lastUpdate: moment(new Date(json.dt * 1000)).format("YYYY/MM/DD hh:mm:ss"),
    };

    //conn.disconnect();
}