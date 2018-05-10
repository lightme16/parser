import re
import csv
import xml.etree.ElementTree as etree
from pymongo import MongoClient

client = MongoClient()
db = client.test_database
collection = db.test_collection

separator_xml = 'item_data'
separator_csv = '|'

class Parser:
    def __init__(self, filename, collection):
        self.collection = collection
        self.unique_keys = ['id', 'title', 'description']
        self.model = {
            "id": self.parse_id,
            "title": self.parse_title,
        #     "sku_number"
            "url": self.parse_url,
            "image_url": self.parse_image_url,
        #     "buy_url"
            "description": self.parse_description,
        #     "discount"
        #     "discount_type"
        #     "currency"
        #     "retail_price"
        #     "sale_price"
        #     "brand"
        #     "manufacture"
        #     "shipping"
        #     "availability"
        #     "sizes"
        #     "materials"
            "colors": self.parse_colors,
        #     "style"
            "gender_group": self.parse_gender_group,
        #     "age_group"
        }
        self._main(filename)

    def _main(self, filename):
        if re.match(r'.+\.xml', filename):
            self._readlines_xml(filename)
        if re.match(r'.+\.txt', filename):
            self._readlines_csv(filename)

    def _readlines_csv(self, filename):
        with open(filename, 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=separator_csv)
            for row in reader:
                tokens = {i: row[i] for i in range(len(row)) if row[i] != ''}
                self._parser(tokens)



    def _readlines_xml(self, filename):
        for event, elem in etree.iterparse('products.xml', events=('start', 'end')):
            if event == 'start' and elem.tag == separator_xml:
                tokens = {}
                self._lexer_xml(elem, tokens)
                self._parser(tokens)

    def _lexer_xml(self, element, tokens):
        text = element.text
        if text and text != '\n':
            tokens[element.tag] = text
        for i in element:
            self._lexer_xml(i, tokens)

    def _parser(self, tokens):
        document = {key: [] for key in self.model.keys() if key not in self.unique_keys}
        pop_keys = [] # this must be remade some better way
        for model_key, model_parser in self.model.items():
            for tokens_key, tokens_value in tokens.items(): # don't know how to be with embedded cycle but I hope the model isn't long
                if model_parser(str(tokens_key), tokens_value):
                    if model_key in self.unique_keys:
                        document[model_key] = tokens_value
                    else:
                        document[model_key].append(tokens_value)
                    pop_keys.append(tokens_key)
        rest_tokens = {key: value for key, value in tokens.items() if key not in pop_keys}
        if rest_tokens:
            document['other_props'] = {}
            for key, value in tokens.items():
                document['other_props'][str(key)] = value

        if document.get('id'):
            print(document) #
            self.collection.insert_one(document) # need to specify id but id parser still need to be fixed

    def parse_id(self, key, value):
        if 'id' in key:
            return True
        pat = r'\b\d{7,15}\b'
        return re.match(pat, value)

    def parse_title(self, key, value):
        if 'title' in key:
            return True
    def parse_url(self, key, value):
        pat1 = r'\bhttp://.*'
        pat2 = r'\bhttps://.*'
        return 'image' not in value and (re.match(pat1, value) or re.match(pat2, value))
    def parse_image_url(self, key, value):
        if 'image' in key and 'url' in key:
            return True
        pat1 = r'\bhttp://.*'
        pat2 = r'\bhttps://.*'
        return 'image' in value and (re.match(pat1, value) or re.match(pat2, value))
    def parse_buy_url(self, str):
        pass
    def parse_description(self, key, value):
        if 'desc' in key:
            return True
        return len(value) > 100
    def parse_discount(self, str):
        pass
    def parse_currency(self, str):
        pass
    def parse_price(self, str):
        pass
    def parse_brand(self, str):
        pass
    def parse_manufacture(self, str):
        pass
    def parse_shipping(self, str):
        pass
    def parse_sizes(self, str):
        pass
    def parse_materials(self, str):
        pass
    def parse_colors(self, key, value):
        for pat in [r'\bblack\b', r'\bwhite\b', r'\bwhite\b', r'\bred\b', r'\bblue\b', r'\bgreen\b', r'\byellow\b', r'\bbrown\b', r'\bgrey\b']:
            if re.match(pat, value, re.IGNORECASE):
                return True

    def parse_gender_group(self, key, value):
        for pat in [r'\bmen\b', r'\bwomen\b', r'\bno gender\b']:
            if re.match(pat, value, re.IGNORECASE):
                return True

    def parse_age_group(self, str):
        pass

def main():
    client = MongoClient()
    db = client.parser_db
    collection = db.parsed
    Parser('products.xml', collection)
    Parser('products.txt', collection)


if __name__ == '__main__':
    main()