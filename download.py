import time
import json
import requests
import argparse
import warnings
import pandas as pd


# It turns out Blue Nile's API will only let us grab 1000 diamonds
# associated with each query. To get around this, let's use price to
# page through the results.
# 1. Get first thousand diamonds in query.
# 2. Use diamond with highest price in that query to seed the next
#    query.


def process_entry(d):
    return {
        "carat": float(d["carat"][0]),
        "clarity": d["clarity"][0],
        "color": d["color"][0],
        "culet": d["culet"][0],
        "cut": d["cut"][0]["label"],
        "date": d["date"][0],
        "depth": float(d["depth"][0]),
        "fluorscence": d["fluorescence"][0],
        "id": d["id"],
        "lxwRatio": float(d["lxwRatio"][0]),
        "polish": d["polish"][0],
        "price": int(float(d["price"][0][1:].replace(",", "."))),
        #"pricePerCarat": float(d["pricePerCarat"][0][1:]),
        "shapeName": d["shapeName"][0],
        "symmetry": d["symmetry"][0],
        "table": float(d["table"][0])
    }


def diamonds(params):
    assert params['sortColumn'] == 'price' and params['sortDirection'] == 'asc'

    landing_page = requests.get('http://www.bluenile.com/')
    url = 'http://www.bluenile.com/api/public/diamond-search-grid/v2'
    result = []
    iteration = 1
    while True:
        response = requests.get(url, params, cookies=landing_page.cookies)
        try:
            d = json.loads(response.text)
        except Exception as e:
            print(response.text)
            return result
        
        print("iteration: {}, diamonds left: {}".format(iteration, d['countRaw']))
        iteration += 1

        last_page = params['pageSize'] >= d['countRaw']

        it_results = [process_entry(entry) for entry in d["results"]]
        max_price = it_results[-1]['price']
        min_price = it_results[0]['price']

        if last_page:
            result += it_results
            break
        else:
            assert min_price < max_price, 'There are over %d diamonds with these characteristics at this price %d.' % (params['pageSize'], min_price)
            result += [x for x in it_results if x['price'] < max_price]
            params['minPrice'] = max_price
        time.sleep(60)  # api limit
    return result


def parse_arguments():
    parser = argparse.ArgumentParser()

    shapes = [
        "RD",  # round
        "PR",  # princess
        "EC",  # emerald
        "AS",  # asscher
        "CU",  # cushion
        "MQ",  # marquise
        "RA",  # radiant
        "OV",  # oval
        "PS",  # pear
        "HS",  # heart
    ]
    parser.add_argument('--shape', nargs='+', choices=shapes)

    parser.add_argument('--minPrice', type=int)
    parser.add_argument('--maxPrice', type=int)
    parser.add_argument('--minCarat', type=float)
    parser.add_argument('--maxCarat', type=float)

    cuts = ['Good', 'Very Good', 'Ideal', 'Signature Ideal']
    colors = ['J', 'I', 'H', 'G', 'F', 'E', 'D']
    clarities = ['SI2', 'SI1', 'VS2', 'VS1', 'VVS2', 'VVS1', 'IF', 'FL']
    select_one = [
        ('minCut', cuts),
        ('maxCut', cuts),
        ('minColor', colors),
        ('maxColor', colors),
        ('minClarity', clarities),
        ('maxClarity', clarities),
    ]
    for key, choices in select_one:
        parser.add_argument('--%s' % key, choices=choices)

    arguments_with_defaults = [
        ('startIndex', 0),
        ('pageSize', 1000),
        ('country', 'USA'),
        ('language', 'en-us'),
        ('currency', 'USD'),
        ('sortColumn', 'price'),
        ('sortDirection', 'asc'),
    ]
    for k, v in arguments_with_defaults:
        parser.add_argument('--%s' % k, default=v, type=type(v))

    args = parser.parse_args()
    d = {k:v for k, v in args.__dict__.items() if v is not None}
    return d


def main():
    params = parse_arguments()
    l = diamonds(params)
    if len(l) == 0:
        print("No Data Downloaded")
        return
    df = pd.DataFrame(l)
    file_name = str(pd.datetime.now()) + ".csv"
    df.to_csv(file_name, index=False)


if __name__ == '__main__':
    main()
