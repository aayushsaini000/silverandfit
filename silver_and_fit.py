import traceback
import re
import csv
import json
import time
import scrapy
import requests
from lxml.html import fromstring
from scrapy.crawler import CrawlerProcess
from uszipcode import SearchEngine
import re
import urllib

# PROXY = '37.48.118.90:13042'
PROXY = "45.79.220.141:3128"


def get_proxies_from_free_proxy():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.content)
    proxies = set()
    for i in parser.xpath('//tbody/tr'):
        if i.xpath('.//td[3][text()="US"]') and\
           i.xpath('.//td[7][contains(text(),"yes")]'):
            ip = i.xpath('.//td[1]/text()')[0]
            port = i.xpath('.//td[2]/text()')[0]
            proxies.add("{}:{}".format(ip, port))
            if len(proxies) == 20:
                return proxies
    return proxies


def get_states():
    return [
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
        "Connecticut", "Delaware", "District of Columbia", "Florida",
        "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts",
        "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico",
        "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma",
        "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island",
        "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
        "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin",
        "Wyoming"
    ]


def get_zip_codes_map():
    search = SearchEngine()
    zipcodes = list()
    for state in get_states():
    # for state in ['New York']:
        final_response = list()
        response = search.by_state(state, returns=2000)
        for r in response:
            if r.major_city not in [x.major_city for x in final_response]:
                final_response.append(r)
        for res in response:
            if res:
                zipcodes.append({
                    'zip_code': res.zipcode,
                    'latitude': res.lat,
                    'longitude': res.lng,
                    'city': res.major_city,
                    'state': res.state
                })
    return sorted(zipcodes, key=lambda k: k['state'])


class ExtractItem(scrapy.Item):
    docId = scrapy.Field()
    clinicId = scrapy.Field()
    netCode = scrapy.Field()
    facilityName = scrapy.Field()
    facilityType = scrapy.Field()
    line1 = scrapy.Field()
    line2 = scrapy.Field()
    city = scrapy.Field()
    state = scrapy.Field()
    zip = scrapy.Field()
    proximity = scrapy.Field()
    phone = scrapy.Field()
    fax = scrapy.Field()
    email = scrapy.Field()
    website = scrapy.Field()
    amenities = scrapy.Field()
    classes = scrapy.Field()
    gender = scrapy.Field()
    acceptingNewMembers = scrapy.Field()
    enrolled = scrapy.Field()
    ashAcceptsFeeStatusSID = scrapy.Field()
    isActiveOptionsInstructor = scrapy.Field()
    doesNotParticipateInAllHealthPlans = scrapy.Field()
    activeOptionsInstructorClasses = scrapy.Field()
    displayExerciseClasses = scrapy.Field()
    minimumAge = scrapy.Field()
    hasGuestPass = scrapy.Field()


class SilverAndFitSpider(scrapy.Spider):
    name = "silver_and_fit_spider"
    allowed_domains = ["silverandfit.com"]
    scraped_data = list()
    fieldnames = [
        'docId', 'clinicId', 'netCode', 'facilityName', 'facilityType',
        'line1', 'line2', 'city', 'state', 'zip', 'proximity', 'phone',
        'fax', 'email', 'website', 'amenities', 'classes', 'gender',
        'acceptingNewMembers', 'enrolled', 'ashAcceptsFeeStatusSID',
        'isActiveOptionsInstructor', 'doesNotParticipateInAllHealthPlans',
        'activeOptionsInstructorClasses', 'displayExerciseClasses',
        'minimumAge', 'hasGuestPass'
    ]

    
    def start_requests(self):
        base_url = "https://www.silverandfit.com/search"
        zip_codes_map = get_zip_codes_map()
        for index, zip_code_map in enumerate(zip_codes_map, 1):
            url = "https://www.silverandfit.com/"
            meta = {'latitude': zip_code_map['latitude'], "longitude": zip_code_map['longitude']}
            yield scrapy.Request(
                url=url,
                callback=self.parse_apitoken,
                dont_filter=True,
                meta=meta
            )
    
    def parse_apitoken(self, response):
        rgx = r'"apiToken":"(.*?)"'
        res = re.search(rgx, response.text)
        if res:
            token = res.group(1)
        else:
            token = ""
        headers = {"Authorization": f"Bearer {token}"}
        url = "https://networksearch.api.ashcompanies.com/api/v1/locations?"
        params = {"amenities": 0, "distance": 60, "latitude": response.meta['latitude'], "filters": 0, 
        "locationType": 0, "longitude": response.meta['longitude'], "siteId": 8}
        full_url = url + urllib.parse.urlencode(params)
        meta = {"token_header": headers}

        yield scrapy.Request(
                url=full_url,
                headers=headers,
                callback=self.parse_search,
                dont_filter=True,
                meta=meta
        )
    
    def parse_search(self, response):
        headers = response.meta['token_header']
        if not response.status == 200:
            return
        json_response = json.loads(response.text)
        if 'error' in json_response:
            print(json_response['errorMessage'])
            return
        locations_list = json_response['locations']
        for location in locations_list:
            clinicId = str(location['clinicId'])
            url = "https://networksearch.api.ashcompanies.com/api/v1/clinics/" + clinicId + "?siteId=8"

            yield scrapy.Request(
                    url=url,
                    headers=headers,
                    callback=self.parse_clinic,
                    dont_filter=True,
            )

    def parse_clinic(self, response):
        if not response.status == 200:
            return
        json_response = json.loads(response.text)
        if 'error' in json_response:
            print(json_response['errorMessage'])
            return
        clinic_dict = json_response['clinic']
        if clinic_dict.get('docId') not in self.scraped_data:
            item = ExtractItem()
            # dict_to_write = {k: clinic_dict[k] for k in self.fieldnames}
            # dict_to_write = {d[i]: 0 if k in clinic_dict else None for k in self.fieldnames}
            item['docId'] = clinic_dict.get('docId', None)
            item['clinicId'] = clinic_dict.get('clinicId', None)
            item['netCode'] = clinic_dict.get('netCode', None)
            item['facilityName'] = clinic_dict.get('clinicName', None)
            item['facilityType'] = clinic_dict.get('type', None)
            item['line1'] = clinic_dict.get('address1', None)
            item['line2'] = clinic_dict.get('address2', None)
            item['city'] = clinic_dict.get('city', None)
            item['state'] = clinic_dict.get('state', None)
            item['zip'] = clinic_dict.get('zip', None)
            item['proximity'] = clinic_dict.get('proximity', None)
            item['phone'] = clinic_dict.get('phone', None)
            item['fax'] = clinic_dict.get('fax', None)
            item['email'] = clinic_dict.get('email', None)
            item['website'] = clinic_dict.get('webUrl', None)
            if clinic_dict.get('amenities', None):
                amenities = ",".join([k['techniqDesc'] for k in  clinic_dict['amenities']])
            else:
                amenities = None    
            item['amenities'] = amenities
            if clinic_dict.get('schedule', None):
                try:
                    classes_list = clinic_dict['schedule']['classes']
                    if classes_list:
                        days_list = classes_list[0]['days']
                        if days_list:
                            classes = ""
                            for day_dict in days_list:
                                if day_dict['hours']:
                                    if len(day_dict['hours']) > 1:
                                        hours = ",".join([f"{k['from']}-{k['to']}" for k in day_dict['hours']])
                                    else:
                                        hours = f"{day_dict['hours'][0]['from']}-{day_dict['hours'][0]['to']}"
                                    classes += f"{day_dict['weekDay']}({hours})"
                        else:
                            classes = None
                    else:
                        classes = None        
                except Exception as e:
                    print('error while formating classes: %s',  str(e))
                    classes = None    
            else:
                classes = None                                    
            item['classes'] = classes
            item['gender'] = clinic_dict.get('clubMbrGenderDesc', None)
            item['acceptingNewMembers'] = clinic_dict.get('newPatients', None)
            item['ashAcceptsFeeStatusSID'] = clinic_dict.get('ashAcceptsFeeStatusSID', None)
            item['isActiveOptionsInstructor'] = clinic_dict.get('isActiveOptionsInstructor', None)
            item['doesNotParticipateInAllHealthPlans'] = clinic_dict.get('doesNotParticipateInAllHealthPlans', None)
            item['activeOptionsInstructorClasses'] = clinic_dict.get('activeOptionsInstructorClasses', None)
            item['displayExerciseClasses'] = clinic_dict.get('displayExerciseClasses', None)
            item['minimumAge'] = clinic_dict.get('minAgeWithoutGuardian', None)
            item['hasGuestPass'] = clinic_dict.get('guestPass', None)

            self.scraped_data.append(clinic_dict['docId'])
            yield item

def run_spider(no_of_threads, request_delay):
    settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
        'ITEM_PIPELINES': {
            'pipelines.ExtractPipeline': 300,
        },
        'DOWNLOAD_DELAY': request_delay,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
        'RETRY_HTTP_CODES': [403, 429, 500, 503],
        'ROTATING_PROXY_LIST': PROXY,
        'ROTATING_PROXY_BAN_POLICY': 'pipelines.BanPolicy',
        'RETRY_TIMES': 10,
        'LOG_ENABLED': True,

    }
    process = CrawlerProcess(settings)
    process.crawl(SilverAndFitSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 40
    request_delay = 0.01
    run_spider(no_of_threads, request_delay)
