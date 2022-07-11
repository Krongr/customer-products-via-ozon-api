from sqlalchemy import Column, Integer, Text, String, ForeignKey
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Marketplace(Base):
    __tablename__ = 'marketplaces_list'
    id = Column(Integer, primary_key=True)
    mp_name = Column(String, nullable=False)
    description = Column(Text)

class Account(Base):
    __tablename__ = 'account_list'
    id = Column(Integer, primary_key=True, autoincrement=True)
    mp_id = Column(Integer, ForeignKey(Marketplace.id))
    client_id_api = Column(String)
    api_key = Column(Text)

class Category(Base):
    __tablename__ = 'category'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    cat_id = Column(String, nullable=False)
    mp_id = Column(Integer, ForeignKey(Marketplace.id))

class ProductAttributes(Base):
    __tablename__ = 'product_attr'
    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String)
    attribute_id = Column(String)
    value = Column(Text)    
    dictionary_value_id = Column(String)
    complex_id = Column(String)
    mp_id = Column(Integer, ForeignKey(Marketplace.id))
    db_i = Column(String)  # Index: combined product ID and attribute ID value

class CategoryAttributes(Base):
    __tablename__ = 'cat_list'
    id = Column(Integer, primary_key=True, autoincrement=True)
    chid = Column(String)  # Attribute ID
    name = Column(String)
    is_required = Column(String)
    is_collection = Column(String)
    type = Column(String)
    description = Column(Text)
    dictionary_id = Column(String)
    group_name = Column(String)
    cat_id = Column(String)
    db_i = Column(String)  # Index: combined category ID and attribute ID value

class AttributeDictionaryValue(Base):
    __tablename__ = 'attr_param_list'
    id = Column(Integer, primary_key=True, autoincrement=True)
    chid = Column(String)  # Attribute ID
    value = Column(Text)
    picture = Column(String)
    info = Column(String)
    attr_param_id = Column(String)  # Dictionary value ID
    db_i = Column(String)  # Index: combined attribute ID and dictionary value ID value
