module.exports = cds.service.impl(srv => {
    srv.on("fetchCurrentWeatherData", _fetchCurrentWeatherData)
})

const fetch = require('node-fetch');
const { v4: uuidv4 } = require('uuid');
const moment = require('moment');
const { array } = require('@sap/cds');

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
    /*
    conn.connect(conn_parms);
    try {
        conn.exec('CREATE TABLE WEATHERBYPOSTALCODE(ID NVARCHAR(50), POSTALCODE NVARCHAR(10), WEATHERCONDITIONSID Integer, TEMPERATURE Double, SOURCEUPDATE Timestamp, PRIMARY KEY (ID))');
    } catch (error) {
        console.log("Table WEATHERBYPOSTALCODE already exists");
    }

    try {
        conn.exec('CREATE TABLE WEATHERCONDITIONS(ID INTEGER, MAIN NVARCHAR(50), DESCRIPTION NVARCHAR(50), PRIMARY KEY (ID));');
    } catch (error) {
        console.log("Table WEATHERCONDITIONS already exists");
    }
    conn.disconnect();
    */
    var cities = selectCities(conn_parms, conn);
    insertWeather(conn_parms, conn, await cities);
}

async function selectCities(conn_parms, conn) {
    conn.connect(conn_parms);
    var result = await conn.exec('SELECT * FROM "WEATHERDATA"."Relevant_Cities_for_API"');
    console.log(result.length + " relevant cities in source table");
    conn.disconnect();
    return result;
}

// get weather for a city (ZIP code + country code) via OpenWeatherMap API
async function getWeather(conn_parms, conn, city) {
    var key = config.keys.crawler[0];
    console.log(city);
    const url = config.url.replace(/%s/g, encodeURI(`${city.Postalcode}`)).replace(/%c/g, encodeURI(`${city.Country}`))
        .replace(/%k/g, encodeURI(key));

    const response = await fetch(url);
    const openWeatherJson = await response.json();

    let postalCode = city.Postalcode;
    return {
        postalCode: postalCode.toString(),
        weatherConditionID: openWeatherJson.weather[0].id,
        weatherConditionMain: openWeatherJson.weather[0].main,
        weatherConditionDescription: openWeatherJson.weather[0].description,
        temperature: openWeatherJson.main.temp,
        sourceUpdate: moment(new Date(openWeatherJson.dt * 1000)).format("YYYY/MM/DD hh:mm:ss"),

    };
}


async function insertWeather(conn_parms, conn, weatherResult) {
    conn.connect(conn_parms);
    conn.exec("SET 'TIMESTAMP_FORMAT' = 'YYYY/MM/DD HH:MI:SS'")
    var execArray = [];
    for (var i = 0; i < weatherResult.length; i++) {
        var id = uuidv4().toString();
        const weatherData = await getWeather(conn_parms, conn, weatherResult[i]);
        var sqlWeather = `UPSERT WEATHERBYPOSTALCODE VALUES('${id}', ${weatherData.postalCode}, ${weatherData.weatherConditionID}, ${weatherData.temperature}, '${weatherData.sourceUpdate}') WHERE POSTALCODE = ${weatherData.postalCode} AND SOURCEUPDATE = '${weatherData.sourceUpdate}'`;
        var sqlConditions = `UPSERT WEATHERCONDITIONS VALUES(${weatherData.weatherConditionID}, '${weatherData.weatherConditionMain}', '${weatherData.weatherConditionDescription}') WITH PRIMARY KEY`;
        console.log("i = " + i)
        execArray.push(sqlExec(sqlWeather, conn));
        execArray.push(sqlExec(sqlConditions, conn));
    }
    await Promise.all(execArray);
    conn.disconnect();
}

async function sqlExec(sqlStatement, conn) {
    return new Promise ((resolve, reject) => {conn.exec(sqlStatement, function (err, affectedRows) {
        if (err) return reject(err);
        resolve(affectedRows);
        });
    });
}