import xml.etree.ElementTree as ET
import re
import collections
import pprint

import csv
import codecs
import cerberus
filename=r'F:\boston_massachusetts.osm\boston_samplefile'
SCHEMA = {
    'node': {
        'type': 'dict',
        'schema': {
            'id': {'required': True, 'type': 'integer', 'coerce': int},
            'lat': {'required': True, 'type': 'float', 'coerce': float},
            'lon': {'required': True, 'type': 'float', 'coerce': float},
            'user': {'required': True, 'type': 'string'},
            'uid': {'required': True, 'type': 'integer', 'coerce': int},
            'version': {'required': True, 'type': 'string'},
            'changeset': {'required': True, 'type': 'integer', 'coerce': int},
            'timestamp': {'required': True, 'type': 'string'}
        }
    },
    'node_tags': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'k': {'required': True, 'type': 'string'},
                'v': {'required': True, 'type': 'string'},
            }
        }
    },
    'way': {
        'type': 'dict',
        'schema': {
            'id': {'required': True, 'type': 'integer', 'coerce': int},
            'user': {'required': True, 'type': 'string'},
            'uid': {'required': True, 'type': 'integer', 'coerce': int},
            'version': {'required': True, 'type': 'string'},
            'changeset': {'required': True, 'type': 'integer', 'coerce': int},
            'timestamp': {'required': True, 'type': 'string'}
        }
    },
    'way_nodes': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'ref': {'required': True, 'type': 'integer', 'coerce': int}
            }
        }
    },
    'way_tags': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'k': {'required': True, 'type': 'string'},
                'v': {'required': True, 'type': 'string'},
            }
        }
    }
}

tags_dict={}
mapping={'St':'Street','St.':'Street','Rd':'Road','Rd.':'Road','Ave':'Avenue','H':'Highway','Pkwy':'Parkway'}

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
            "Trail", "Parkway", "Commons"]
LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
#获取field list区间
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
def get_fields(filename,tag):
    for event, elem in ET.iterparse(filename):
        if elem.tag==tag and elem.attrib is not None:
            field_list=list(elem.attrib.keys())
    return field_list
NODE_FIELDS=get_fields(filename,'node')
WAY_FIELDS=get_fields(filename,'way')
def get_child_fields(filename,tag,child_tag):
    for event,elem in ET.iterparse(filename):
        if elem.tag==tag:
            for tag in elem.iter(child_tag):
                if tag.attrib is not None:
                    field_list=list(tag.attrib.keys())
    return field_list

NODE_TAGS_FIELDS=get_child_fields(filename,'node','tag')
WAY_TAGS_FIELDS=get_child_fields(filename,'way','tag')
WAY_NODES_FIELDS=get_child_fields(filename,'way','nd')
#获取field list区间
#修改街道名
def audit_street_types(street_types, street_name):

    m=street_type_re.search(street_name)
    if m:
        street_type=m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)
    return street_types
street_types=collections.defaultdict(set)
def get_mapping(street_types,filename):
    for event, elem in ET.iterparse(filename):
        if elem.tag=='way':
            for tag in elem.iter('tag'):
                if tag.attrib['k']=='addr:street':
                    audit_street_types(street_types,tag.attrib['v'])
    return dict(street_types)
def audit_street_name(name):
    part_of_audit=name.split(' ')[-1]
    if part_of_audit in mapping:
        return name.replace(part_of_audit,mapping[part_of_audit])


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS, problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements
    tags=[tag_node_shape(element,t) for t in element.iter('tag')]
    if element.tag=='node':
        node_attribs={f:element.attrib[f] for f in node_attr_fields}
        return {'node':node_attribs,'node_tags':tags}
    if element.tag=='way':
        way_attribs={f:element.attrib[f] for f in way_attr_fields}
        way_nodes=[way_nodes_shape(element,i, node) for i,node in enumerate(element.iter('nd'))]
        return {'way':way_attribs,'way_nodes':way_nodes,'way_tags':tags}

def tag_node_shape(element,tag):
    if PROBLEMCHARS.search(tag.attrib['k']):
        return None
    tag={
        'id': element.attrib['id'],
        'key':tag.attrib['k'],
        'value':tag.attrib['v'],
        'type':'regular'
        }
    if LOWER_COLON.search(tag['key']):
        tag['type'], _,tag['key']=tag['key'].partition(':')
    return tag

def way_nodes_shape(element,i,node):
    return {
        'id':element.attrib['id'],
        'node_id':node.attrib['ref'],
        'position':i
        }
#辅助函数
def get_element(filename,tags=('node','way','relation')):
    context=ET.iterparse(filename,events=('start','end'))
    _,root =next(context)
    for event,elem in context:
        if event=='end' and elem.tag in tags:
            yield elem
            root.clear()
def validate_element_generator(errors):
    for errors_tuple in errors.items():
        yield errors_tuple
def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field,errors=next(validate_element_generator(validator.errors))
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)

        raise Exception(message_string.format(field, error_string))
        
class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, str) else v) for k, v in row.items()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)
#主函数
def process(file_in,validate):
    with codecs.open(r'F:\boston_massachusetts.osm\node_path.csv', 'w') as nodes_file, \
         codecs.open(r'F:\boston_massachusetts.osm\node_tag_path.csv', 'w') as nodes_tags_file, \
         codecs.open(r'F:\boston_massachusetts.osm\way_path.csv', 'w') as ways_file, \
         codecs.open(r'F:\boston_massachusetts.osm\way_node_path.csv', 'w') as way_nodes_file, \
         codecs.open(r'F:\boston_massachusetts.osm\way_tag_path.csv', 'w') as way_tags_file:
        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)
        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()
        validator = cerberus.Validator()
        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)
                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


process(filename,validate=True)

