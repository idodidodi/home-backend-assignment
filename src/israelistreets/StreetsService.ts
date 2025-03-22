import axios, { Axios } from 'axios';
import { omit } from 'lodash';
import { cities, city, englishNameByCity } from './cities';

interface ApiStreet {
    _id: number
    region_code: number
    region_name: string
    city_code: number
    city_name: string
    street_code: number
    street_name: string
    street_name_status: string
    official_code: number
}

type ApiStreetWithoutId = Omit<ApiStreet, '_id'>;

export interface Street extends ApiStreetWithoutId {
    streetId: number
}

export class StreetsService {
    private static _axios: Axios;
    private static get axios() {
        if (!this._axios) {
            this._axios = axios.create({})
        }
        return this._axios
    }

    private static getHebrewCityName(englishName: string): string | undefined {
        return Object.entries(englishNameByCity).find(([_, english]) =>
            english === englishName
        )?.[0];
    }

    private static getApiFields(): (keyof ApiStreet)[] {
        return ['region_code', 'region_name', 'city_code', 'city_name',
            'street_code', 'street_name', 'street_name_status',
            'official_code'] satisfies (keyof ApiStreet)[];
    }

    static validateAndGetCity(cityArg: string | undefined): { englishName: city, hebrewName: string } {
        if (!cityArg) {
            throw new Error('Please provide a city name in English.');
        }

        const hebrewName = this.getHebrewCityName(cityArg);
        if (!hebrewName) {
            throw new Error(`City "${cityArg}" not found in the list of supported cities.`);
        }

        return {
            englishName: cityArg as city,
            hebrewName
        };
    }

    static async getStreetsInCity(cityArg: string): Promise<{ city: city, streets: Street[] }> {
        const { englishName, hebrewName } = this.validateAndGetCity(cityArg);
        
        const res = (await this.axios.post('https://data.gov.il/api/3/action/datastore_search', { 
            resource_id: `1b14e41c-85b3-4c21-bdce-9fe48185ffca`, 
            filters: { city_name: hebrewName },
            fields: ["_id", "region_code", "region_name", "city_code", "city_name",
                "street_code", "street_name", "street_name_status", "official_code"],
            limit: 100000
        })).data

        const results = res.result.records
        if (!results || !results.length) {
            throw new Error('No streets found for city: ' + englishName)
        }
        const streets: Street[] = results.map((street: ApiStreet) => {
            return { 
                streetId: street._id,
                street_name: street.street_name.trim(),
                region_code: street.region_code,
                region_name: street.region_name.trim(),
                city_code: street.city_code,
                city_name: englishName,
                street_code: street.street_code,
                street_name_status: street.street_name_status,
                official_code: street.official_code
            }
        })
        return { city: englishName, streets }
    }

    static async getStreetInfoById(id: number) {
        const res = (await this.axios.post('https://data.gov.il/api/3/action/datastore_search', {
            resource_id: `1b14e41c-85b3-4c21-bdce-9fe48185ffca`,
            filters: { _id: id },
            fields: this.getApiFields(),
            limit: 1
        })).data

        const results = res.result.records
        if (!results || !results.length) {
            throw new Error('No street found for id: ' + id)
        }
        const dbStreet: ApiStreet = results[0]
        const cityName = englishNameByCity[dbStreet.city_name]
        const street: Street = {
            ...omit(dbStreet, '_id'),
            streetId: dbStreet._id,
            city_name: cityName,
            region_name: dbStreet.region_name.trim(),
            street_name: dbStreet.street_name.trim()
        }
        return street
    }
}


// API request example:
// {
//     "resource_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
//     "filters": {},
//     "q": "",
//     "distinct": true,
//     "plain": true,
//     "limit": 10,
//     "offset": 0,
//     "fields": [],
//     "sort": "",
//     "include_total": true,
//     "records_format": "objects"
//   }