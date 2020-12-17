module.exports = cds.service.impl(srv => {
    srv.on("fetchCurrentWeatherData", _fetchCurrentWeatherData)
})

const fetch = require('node-fetch');

const getVcapService = (serviceName, scope = 'user-provided') => JSON.parse(process.env.VCAP_SERVICES)[scope].filter((e) => e.name === serviceName);
const weatherApiConfig = getVcapService('open-weather-map')[0].credentials;
const dwcAccess = getVcapService('dwc-weather-data-space')[0].credentials;

async function _fetchCurrentWeatherData(req) {
    const hana = require('@sap/hana-client');
    var conn = hana.createConnection();
    var conn_parms = {
        serverNode: dwcAccess.host_and_port,
        encrypt: true,
        uid: dwcAccess.user,
        pwd: dwcAccess.password,
    };

    var cities = selectCities(conn_parms, conn);
    insertWeather(conn_parms, conn, await cities);
}

async function selectCities(conn_parms, conn) {
    conn.connect(conn_parms);
    var result = await conn.exec('SELECT * FROM "' + dwcAccess.schema + '"."Relevant_Cities_for_API"');
    console.log(result.length + " relevant cities in source table");
    conn.disconnect();
    return result;
}

// get weather for a city (ZIP code + country code) via OpenWeatherMap API
async function getWeather(conn_parms, conn, city) {
    var key = weatherApiConfig.keys.crawler[0];
    console.log(city);
    const url = weatherApiConfig.url.replace(/%s/g, encodeURI(`${city.Postalcode}`)).replace(/%c/g, encodeURI(`${city.Country}`))
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
        sourceUpdate: convertTimestamp(new Date(openWeatherJson.dt*1000)),

    };
}

async function insertWeather(conn_parms, conn, weatherResult) {
    conn.connect(conn_parms);
    conn.exec("SET 'TIMESTAMP_FORMAT' = 'YYYY/MM/DD HH:MI:SS'")
    var execArray = [];
    var currentTimestamp = convertTimestamp(new Date());

    for (var i = 0; i < weatherResult.length; i++) {
        const weatherData = await getWeather(conn_parms, conn, weatherResult[i]);
        var sqlWeather = `UPSERT API_WEATHERBYPOSTALCODE VALUES(${weatherData.postalCode}, ${weatherData.weatherConditionID}, ${weatherData.temperature}, '${weatherData.sourceUpdate}', '${currentTimestamp}') WITH PRIMARY KEY`;
        var sqlConditions = `UPSERT API_WEATHERCONDITIONS VALUES(${weatherData.weatherConditionID}, '${weatherData.weatherConditionMain}', '${weatherData.weatherConditionDescription}') WITH PRIMARY KEY`;
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

function convertTimestamp(inputTimestamp) {
    let date = ("0" + inputTimestamp.getDate()).slice(-2);
    let month = ("0" + (inputTimestamp.getMonth() + 1)).slice(-2);
    let year = inputTimestamp.getFullYear();
    let hours = `${inputTimestamp.getHours()}`.padStart(2, '0')
    let minutes = `${inputTimestamp.getMinutes()}`.padStart(2, '0')
    let seconds = `${inputTimestamp.getSeconds()}`.padStart(2, '0')
    var currentTimestamp = year + "/" + month + "/" + date + " " + hours + ":" + minutes + ":" + seconds;
    return currentTimestamp;
}