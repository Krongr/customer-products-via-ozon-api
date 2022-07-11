import requests
import sqlalchemy
from db_client import DbClient
from models import (ProductAttributes, Category, CategoryAttributes,
                       AttributeDictionaryValue)
from ozon_api import OzonApi
from utils import write_event_log


# DB settings:
TYPE= 'postgresql'
NAME= ''
HOST= ''
PORT= ''
USER= ''
PASSWORD= ''


def collect_product_ids(ozon:OzonApi, product_ids:list=None, 
                        last_id:str='')->list:
    """Returns a list of client's product ids.
    Iterates over large lists of products with recursive self-calls.
    """
    product_ids = product_ids or []
    try:
        response = ozon.product_list(last_id=last_id)
    except requests.exceptions.ConnectionError as error:
        write_event_log(error, 'ozon.product_list')
        return product_ids

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as error:
        write_event_log(error, 'collect_product_ids', response.json())
        return product_ids

    try:
        result = response.json()['result']
    except KeyError as error:
        write_event_log(error, 'collect_product_ids', response.json())
        return product_ids

    try:
        products = result['items']
    except KeyError as error:
        write_event_log(error, 'collect_product_ids', response.json())
        return product_ids
    
    try:
        assert isinstance(products, list)
    except AssertionError:
        write_event_log(
            f'{type(products)} is not "list" object',
            'collect_product_ids',
        )
        return product_ids

    if products:
        for _entry in products:
            try:
                product_ids.append(_entry['product_id'])
            except KeyError as error:                
                write_event_log(error, 'collect_product_ids', response.json())
                return product_ids
        try:        
            return collect_product_ids(ozon, product_ids, result['last_id'])
        except KeyError as error:
            write_event_log(error, 'collect_product_ids', response.json())
            return product_ids
    else:
        return product_ids

def collect_products_attributes(ozon:OzonApi, product_ids:list,)->list:
    """Returns a list of attributes of the client's products.
    """
    _product_ids = (product_ids if isinstance(product_ids, list) 
                else [product_ids])
    products_with_attributes = []
    for i in range(0, len(_product_ids), 50):
        try:
            response = ozon.product_attributes(_product_ids[i:i+50])
        except requests.exceptions.ConnectionError as error:
            write_event_log(error, 'ozon.product_attributes')
            continue

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            write_event_log(
                error,
                'collect_products_attributes',
                response.json(),
            )
            continue

        try:
            result = response.json()['result']
        except KeyError as error:
            write_event_log(
                error,
                'collect_products_attributes',
                response.json(),
            )
            continue

        try:        
            for _entry in result:
                products_with_attributes.append(_entry)
        except TypeError as error:
            write_event_log(
                error,
                'collect_products_attributes',
            )

    return products_with_attributes

def add_product_attribute_records(ozon:OzonApi, db:DbClient,
                                  db_session, product:dict):
    """Gets the product description. Returns a DB session with
    created product attributes and description records.
    """
    # Get product description:
    try:
        response = ozon.product_description(product.get('id'))
    except requests.exceptions.ConnectionError as error:
        write_event_log(error, 'ozon.product_description')
        product_description = None

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as error:
        write_event_log(
            error,
            'add_product_attribute_records',
            response.json(),
        )
        product_description = None

    try:
        product_description = response.json()['result']['description']
    except KeyError as error:
        write_event_log(
            error,
            'add_product_attribute_records',
            response.json(),
        )
        product_description = None

    if product_description:
        try:
            db_session = db.add_record(
                db_session=db_session,
                model=ProductAttributes,
                product_id=product['id'],
                attribute_id='description',
                value=product_description, 
                mp_id=1,
                db_i=f"{product['id']}description",
            )
        except KeyError as error:
            write_event_log(
                error,
                'add_product_attribute_records',
            )

    # Get product attributes:
    LIST_ATTRIBUTES = (
        'images',
        'images360',
        'pdf_list',
        'complex_attributes',
    )
    try:
        for _key, _value in product.items():
            if _key in LIST_ATTRIBUTES:
                value_list = []
                for _item in _value:
                    try:
                        value_list.append(_item['file_name'])
                    except KeyError as error:
                        write_event_log(
                            error,
                            'add_product_attribute_records',
                        )
                _complex_value = '|'.join(value_list) if value_list else None
                try:    
                    db_session = db.add_record(
                        db_session=db_session,
                        model=ProductAttributes,
                        product_id=product['id'],
                        attribute_id=_key,
                        value=_complex_value, 
                        mp_id=1,
                        db_i=f"{product['id']}{_key}",
                    )
                except KeyError as error:
                    write_event_log(
                        error,
                        'add_product_attribute_records',
                    )

            elif _key == 'attributes':
                for _attribute in _value:
                    for _item in _attribute.get('values'):
                        try:
                            db_session = db.add_record(
                                db_session=db_session,
                                model=ProductAttributes,
                                product_id=product['id'],
                                attribute_id=_attribute['attribute_id'],
                                value=_item['value'],
                                dictionary_value_id=_item[
                                                'dictionary_value_id'],
                                complex_id=_attribute['complex_id'], 
                                mp_id=1,
                                db_i=(f"{product['id']}"
                                      f"{_attribute['attribute_id']}"),
                            )
                        except KeyError as error:
                            write_event_log(
                                error,
                                'add_product_attribute_records',
                            )

            elif _key not in ('id', 'last_id'):
                try:
                    db_session = db.add_record(
                        db_session=db_session,
                        model=ProductAttributes,
                        product_id=product['id'],
                        attribute_id=_key,
                        value=_value,
                        mp_id=1,
                        db_i=f"{product['id']}{_key}",
                    )
                except KeyError as error:
                    write_event_log(
                        error,
                        'add_product_attribute_records',
                    )
    except TypeError as error:
        write_event_log(
            error,
            'add_product_attribute_records',
        )

    return db_session

def add_category_records(ozon:OzonApi, db:DbClient, category_ids:set,
                         db_session):
    """Returns a DB session with created category records.
    """
    if (hasattr(category_ids, '__iter__') and
        not isinstance(category_ids, str)):
        _category_ids = category_ids
    else:
        _category_ids = [category_ids]

    for _category in _category_ids:
        try:
            response = ozon.category_info(_category)
        except requests.exceptions.ConnectionError as error:
            write_event_log(error, 'ozon.category_info')
            continue

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            write_event_log(error, 'add_category_records', response.json())
            continue

        try:
            _category_info = response.json()['result'][0]
        except (TypeError, KeyError) as error:
            write_event_log(error, 'add_category_records', response.json())
            continue

        if _category_info:
            try:
                db_session = db.add_record(
                    db_session=db_session,
                    model=Category,
                    name=_category_info['title'],
                    cat_id=_category_info['category_id'],
                    mp_id=1,
                )
            except KeyError as error:
                write_event_log(error, 'add_category_records', response.json())
        
    return db_session

def add_category_attribute_records(ozon:OzonApi, db:DbClient,
                    category_ids:set, named_attribute_ids:list, db_session):
    """Returns a DB session with created category attribute records.
    Also returns a dictionary with categories and dictionary attributes ids
    needed to get dictionary values.
    """
    dictionary_attributes = dict()
    if (hasattr(category_ids, '__iter__') and
        not isinstance(category_ids, str)):
        _category_ids = list(category_ids)
    else:
        _category_ids = [category_ids]

    for i in range(0, len(_category_ids), 20):
        try:
            response = ozon.category_attributes(_category_ids[i:i+20])
        except requests.exceptions.ConnectionError as error:
            write_event_log(error, 'ozon.category_attributes')
            continue

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            write_event_log(
                error,
                'add_category_attribute_records',
                response.json(),
            )
            continue

        try:
            _category_attributes = response.json()['result']
        except (TypeError, KeyError) as error:
            write_event_log(error, 'add_category_records', response.json())
            continue

        try:
            for _category in _category_attributes:
                dictionary_attributes[_category['category_id']] = []
                for _attribute in _category['attributes']:
                    db_session = db.add_record(
                        db_session=db_session,
                        model=CategoryAttributes,
                        chid=_attribute['id'],
                        name=_attribute['name'],
                        is_required=_attribute['is_required'],
                        is_collection=_attribute['is_collection'],
                        type=_attribute['type'],
                        description=_attribute['description'],
                        dictionary_id=_attribute['dictionary_id'],
                        group_name=_attribute['group_name'],
                        cat_id=_category['category_id'],
                        db_i=f"{_category['category_id']}{_attribute['id']}"
                    )
                
                    if _attribute['dictionary_id'] != 0:
                        dictionary_attributes[_category['category_id']].append(
                            _attribute['id']
                        )

                for _named_attribute in named_attribute_ids:
                    db_session = db.add_record(
                        db_session=db_session,
                        model=CategoryAttributes,
                        chid=_named_attribute,
                        name=_named_attribute,
                        is_required=True,
                        is_collection=False,
                        type='',
                        description=_named_attribute,
                        dictionary_id=None,
                        group_name=None,
                        cat_id=_category['category_id'],
                        db_i=f"{_category['category_id']}{_named_attribute}"
                    )
        except (TypeError, KeyError) as error:
            write_event_log(
                error,
                'add_category_records',
            )
            continue

    return db_session, dictionary_attributes

def add_dictionary_attribute_value_records(ozon:OzonApi, db:DbClient,
                        category_id, attribute_id, last_value_id:int=None):
    """Creates records of the attribute's dictionary values in the DB.
    """
    try:
        response = ozon.attribute_dictionary_values(
            category_id,
            attribute_id,
            last_value_id,
        )
    except requests.exceptions.ConnectionError as error:
        write_event_log(error, 'ozon.attribute_dictionary_values')
        return

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as error:
        write_event_log(
            error,
            'add_dictionary_attribute_value_records',
            response.json(),
        )
        return

    try:
        _dictionary_values = response.json()['result']
    except KeyError as error:
        write_event_log(
            error,
            'add_dictionary_attribute_value_records',
            response.json(),
        )
        return

    if _dictionary_values:
        db_session = db.start_session()
        for _value in _dictionary_values:
            try:
                db_session = db.add_record(
                    db_session=db_session,
                    model=AttributeDictionaryValue,
                    value=_value['value'],
                    picture=_value['picture'],
                    info=_value['info'],
                    attr_param_id=_value['id'],
                    chid=attribute_id,
                    db_i=f"{attribute_id}{_value['id']}"
                )
            except KeyError as error:
                write_event_log(
                    error,
                    'add_dictionary_attribute_value_records',
                )
        db_session.commit()

    #Remove duplicates
    try:
        db.remove_duplicates(AttributeDictionaryValue.__tablename__, 'db_i')
    except (
        sqlalchemy.exc.InternalError,
        sqlalchemy.exc.IntegrityError,
        sqlalchemy.exc.ProgrammingError,
        sqlalchemy.exc.DataError,
        sqlalchemy.exc.OperationalError,
    ) as error:
        write_event_log(
            error,
            'add_dictionary_attribute_value_records db.remove_duplicates',
        )

    try:
        if response.json()['has_next']:
            add_dictionary_attribute_value_records(
                ozon,
                db,
                category_id,
                attribute_id,
                _value['id'],
            )
    except KeyError as error:
        write_event_log(
            error,
            "'add_dictionary_attribute_value_records' recursive self-call",
            response.json(),
        )


if __name__ == '__main__':
    db = DbClient(TYPE, NAME, HOST, PORT, USER, PASSWORD)
   
    try:
        credentials = db.get_credentials(mp_id=1)
    except (
        sqlalchemy.exc.OperationalError,
        sqlalchemy.exc.InternalError,
        sqlalchemy.exc.ProgrammingError,
    ) as error:
        write_event_log(error, 'DbClient.get_credentials')
        raise error

    for _entry in credentials:
        ozon = OzonApi(_entry['client_id'], _entry['api_key'])

        # Collect client's product ids:
        product_ids = collect_product_ids(ozon)
        try: 
            assert product_ids
        except AssertionError:
            write_event_log(
                f"'product_ids' is empty",
                'collect_product_ids',
            )
            continue
        
        # Collect the attributes of the client's products:
        products_with_attributes = collect_products_attributes(
            ozon, 
            product_ids,
        )
        try:
            assert products_with_attributes
        except AssertionError:
            write_event_log(
                f"'products_with_attributes' is empty",
                'collect_products_attributes',
            )
            continue

        # Record categories and attributes of the client's products:
        category_ids = set()
        named_attribute_ids = []

        db_session = db.start_session()
        for _product in products_with_attributes:
            db_session = add_product_attribute_records(
                ozon,
                db,
                db_session,
                _product,
            )

            try:
                assert _product['category_id'] != 0
            except AssertionError:
                write_event_log(
                    f'Product {_product["id"]} has category_id == 0',
                    'category_ids.add'
                )
            if _product['category_id'] != 0:
                try:
                    category_ids.add(_product['category_id'])
                except KeyError as error:
                    write_event_log(error, 'category_ids.add')
                
        try:
            db_session.commit()
        except (
            sqlalchemy.exc.InternalError,
            sqlalchemy.exc.IntegrityError,
            sqlalchemy.exc.ProgrammingError,
            sqlalchemy.exc.DataError,
            sqlalchemy.exc.OperationalError,
        ) as error:
            write_event_log(error, 'products_with_attributes.commit')
        
        try:
            for _named_attribute_id in products_with_attributes[0]:
                if _named_attribute_id not in ('id', 'attributes', 'last_id'):
                    named_attribute_ids.append(_named_attribute_id)
        except TypeError as error:
            write_event_log(error, 'named_attribute_ids.append')
        
        #Remove duplicates
        try:
            db.remove_duplicates(ProductAttributes.__tablename__, 'db_i')
        except (
            sqlalchemy.exc.InternalError,
            sqlalchemy.exc.IntegrityError,
            sqlalchemy.exc.ProgrammingError,
            sqlalchemy.exc.DataError,
            sqlalchemy.exc.OperationalError,
        ) as error:
            write_event_log(
                error,
                'add_product_attribute_records db.remove_duplicates',
            )
        # End of processing client's products

        try:
            assert category_ids
        except AssertionError:
            write_event_log(
                f"'category_ids' is empty",
                'add_product_attribute_records',
            )
            continue

        # Record the received categories and their attributes
        db_session = db.start_session()
        db_session = add_category_records(ozon, db, category_ids, db_session)
        db_session, dictionary_attributes = add_category_attribute_records(
            ozon,
            db,
            category_ids,
            named_attribute_ids,
            db_session,
        )

        try:
            db_session.commit()
        except (
            sqlalchemy.exc.InternalError,
            sqlalchemy.exc.IntegrityError,
            sqlalchemy.exc.ProgrammingError,
            sqlalchemy.exc.DataError,
            sqlalchemy.exc.OperationalError,
        ) as error:
            write_event_log(error, 'categories.commit')

        #Remove duplicates
        for table_name, partition in (
            (Category.__tablename__, 'cat_id'),
            (CategoryAttributes.__tablename__, 'db_i'),
        ):
            try:
                db.remove_duplicates(table_name, partition)
            except (
                sqlalchemy.exc.InternalError,
                sqlalchemy.exc.IntegrityError,
                sqlalchemy.exc.ProgrammingError,
                sqlalchemy.exc.DataError,
                sqlalchemy.exc.OperationalError,
            ) as error:
                write_event_log(
                    error,
                    'add_category_attribute_records db.remove_duplicates',
                )

        try:
            assert dictionary_attributes
        except AssertionError:
            write_event_log(
                f"'dictionary_attributes' is empty",
                'add_category_attribute_records',
            )
            continue

        # # Record dictionary attribute values (long procces)
        for _category in dictionary_attributes:
            for _attribute in dictionary_attributes[_category]:
                add_dictionary_attribute_value_records(
                    ozon,
                    db,
                    _category,
                    _attribute,
                )
