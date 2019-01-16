const Ix = require('ix')
const Axios = require('axios')
const PapaParse = require('papaparse')
const FSExtra = require('fs-extra')

const location = 'https://raw.githubusercontent.com/everypolitician/everypolitician-data/master/countries.json'

function request(url) {
    return Axios(url)
}

function invert(response) {
    return response.data.map(country => {
        let countryOnly = Object.assign({}, country)
        delete countryOnly.legislatures
        return country.legislatures.map(legislature => {
            let legislatureOnly = Object.assign({}, legislature)
            delete legislatureOnly.legislative_periods
            return legislature.legislative_periods.map(period => {
                return {
                    country: countryOnly,
                    legislature: legislatureOnly,
                    period
                }
            }).flat()
        }).flat()
    }).flat()
}

function filters(item) {
    const valid = ['US', 'CA', 'GB', 'FR', 'DE', 'ES', 'IT', 'IN', 'RU']
    return valid.includes(item.country.code) && item.period.end_date === undefined
}

async function expand(item) {
    console.log(`Fetching ${item.country.name}: ${item.legislature.name}, ${item.period.name}...`)
    const http = await Axios({ url: item.period.csv_url, responseType: 'text' })
    const politicians = PapaParse.parse(http.data, { header: true }).data.filter(politician => politician.id !== '')
    return politicians.map(politician => {
        return Object.assign({}, item, { politician })
    })
}

function format(item) {
    return {
        country: item.country.name,
        legislature: item.legislature.name,
        period: item.period.name,
        name: item.politician.name,
        group: item.politician.group,
        area: item.politician.area,
        gender: item.politician.gender
    }
}

async function write(record) {
    const file = 'everypolitician.csv'
    const fileExists = await FSExtra.pathExists(file)
    if (!fileExists) {
        const header = Object.keys(record)
        await FSExtra.writeFile(file, header + '\n')
    }
    const contents = PapaParse.unparse([Object.values(record)])
    return FSExtra.appendFile(file, contents + '\n')
}

function run() {
    Ix.AsyncIterable.from([location])
        .map(request)
        .flatMap(invert)
        .filter(filters)
        .flatMap(expand)
        .map(format)
        .forEach(write)
        .catch(e => console.error(e.stack))
        .finally(() => console.log('Done!'))
}

run()
