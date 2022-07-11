import requests
import json


class OzonApi():
    def __init__(self, client_id, api_key):
        self.api_url = 'https://api-seller.ozon.ru'
        self.headers = {
            'Content-Type': 'application/json',
            'Client-Id': f'{client_id}',
            'Api-key': f'{api_key}',
        }

    def product_list(self, last_id='', limit=1000):
        """Returns a list of customer's (Client-Id) products placed on Ozon.
        'last_id' can be used to iterate over large lists.
        """
        _url = f'{self.api_url}/v2/product/list'
        _data = {
            'last_id': last_id,
            'limit': limit,
        }
        return requests.post(
            url=_url,
            headers=self.headers,
            data=json.dumps(_data),
        )

    def product_attributes(self, product_ids:list, last_id='', limit=1000):
        """Returns a list of dictionaries with with product attributes.
        Even though it is suggested to use 'last_id' for
        iterating over large lists, method of application is not clear.
        For this reason, the use of lists longer than 50 values
        is not recommended.
        """
        if (hasattr(product_ids, '__iter__') and
            not isinstance(product_ids, str)):
            _product_ids = list(product_ids)
        else:
            _product_ids = [product_ids]

        _url = f'{self.api_url}/v3/products/info/attributes'
        _data = {
            "filter": {
                "product_id": _product_ids,
                "visibility": "ALL",
            },
            'last_id': last_id,
            'limit': limit,
        }
        return requests.post(
            url=_url,
            headers=self.headers,
            data=json.dumps(_data),
        )

    def product_description(self, product_id:int):
        """Returns a dictionary containing product description.
        """
        _url = f'{self.api_url}/v1/product/info/description'
        _data = {
            'product_id': product_id,
        }
        return requests.post(
            url=_url,
            headers=self.headers,
            data=json.dumps(_data),
        ) 

    def category_info(self, category_id:int=None, language='RU'):
        """Returns the category name and subcategories.
        If used without 'category_id' returns the full category tree.
        """
        _url = f'{self.api_url}/v2/category/tree'
        _data = {
            'category_id': category_id,
            'language': language,
        }
        return requests.post(
            url=_url,
            headers=self.headers,
            data=json.dumps(_data),
        )

    def category_attributes(self, category_ids:list,
                            attribute_type='ALL', language='RU'):
        """Returns available attributes for the specified product categories.
        'category_ids' list should not contain more than 20 entries.
        """
        if (hasattr(category_ids, '__iter__') and
            not isinstance(category_ids, str)):
            _category_ids = list(category_ids)
        else:
            _category_ids = [category_ids]

        _url = f'{self.api_url}/v3/category/attribute'
        _data = {
            'attribute_type': attribute_type,
            'category_id': _category_ids,
            'language': language,
        }
        return requests.post(
            url=_url,
            headers=self.headers,
            data=json.dumps(_data),
        )

    def attribute_dictionary_values(self, category_id:int, attribute_id:int,
                           last_value_id:int=None, limit=5000, language='RU'):
        """Returns a list of dictionary values for the specified attribute.
        'last_value_id' can be used to iterate over large lists.
        """
        _url = f'{self.api_url}/v2/category/attribute/values'
        _data = {
            'attribute_id': attribute_id,
            'category_id': category_id,
            'last_value_id': last_value_id,
            'language': language,
            'limit': limit,
        }
        return requests.post(
            url=_url,
            headers=self.headers,
            data=json.dumps(_data),
        )
