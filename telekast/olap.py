#!/usr/bin/env python



def dimension_id_lookup_func(value, dim_table_name, key_field_name, value_field_name, **kwargs):
    if not value:
        return None

    else:
        db_schema = 'olap'
        raw_template = """
            SELECT {field}
            from {schema}.{table}
            where {dim_table_value_field_name} = :source_value""".format(schema=db_schema,
                                                                         field=key_field_name,
                                                                         table=dim_table_name,
                                                                         dim_table_value_field_name=value_field_name)
        template = text(raw_template)
        stmt = template.bindparams(bindparam('source_value', String))

        data_mgr = kwargs['persistence_manager']
        dbconnection = data_mgr.database.engine.connect()
        # db_session = data_mgr.getSession()

        result = dbconnection.execute(stmt, {"source_value": value})
        record = result.fetchone()
        if not record:
            raise Exception('returned empty result set from query: %s where value is %s' % (str(stmt), value))

        return record[0]



class OLAPSchemaDimension(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('fact_table_field_name',
                                            'dim_table_name',
                                            'key_field_name',
                                            'value_field_name',
                                            'primary_key_type',
                                            'id_lookup_function')

        kwreader.read(**kwargs)

        self._fact_table_field_name = kwreader.get_value('fact_table_field_name')
        self._dim_table_name = kwreader.get_value('dim_table_name')
        self._key_field_name = kwreader.get_value('key_field_name')
        self._value_field_name = kwreader.get_value('value_field_name')
        self._pk_type = kwreader.get_value('primary_key_type')
        self._lookup_func = kwreader.get_value('id_lookup_function')


    @property
    def fact_table_field_name(self):
        return self._fact_table_field_name


    @property
    def primary_key_field_name(self):
        return self._key_field_name


    @property
    def primary_key_field_type(self):
        return self._pk_type


    @property
    def value_field_name(self):
        return self._value_field_name

    @property
    def value_field_type(self):
        return self._value_

    def lookup_id_for_value(self, value, **kwargs):
        return self._lookup_func(value,
                                 self._dim_table_name,
                                 self._key_field_name, 
                                 self._value_field_name, 
                                 **kwargs)



class OLAPSchemaFact(object):
    def __init__(self, table_name, pk_field_name, pk_field_type):
        self._table_name = table_name
        self._pk_field = pk_field_name
        self._pk_field_type = pk_field_type

    @property
    def table_name(self):
        return self._table_name

    @property
    def primary_key_field_name(self):
        return self._pk_field

    @property
    def primary_key_field_type(self):
        return self._pk_field_type



class NonDimensionField(object):
    def __init__(self, fname, ftype):
        self._field_name = fname
        self._field_type = ftype


    @property
    def field_name(self):
        return self._field_name


    @property
    def field_type(self):
        return self._field_type



class OLAPSchemaMappingContext(object):
    def __init__(self, schema_fact):
        self._fact = schema_fact
        self._dimensions = {}
        self._direct_mappings = {}

    @property
    def fact(self):
        return self._fact

    @property
    def dimension_names(self):
        return self._dimensions.keys()

    @property 
    def non_dimension_names(self):
        return self._direct_mappings.keys()


    def get_non_dimension_field(self, field_name):
        if not self._direct_mappings.get(field_name):
            #TODO: create custom exception
            raise Exception('No non-dimensional field "%s" registered with mapping context.' % field_name)
        return self._direct_mappings[field_name]
        
    
    def get_dimension(self, dimension_name):
        if not self._dimensions.get(dimension_name):
            #TODO: create custom exception
            raise Exception('No dimension "%s" registered with mapping context.' % dimension_name)
        return self._dimensions[dimension_name]


    def map_src_record_field_to_dimension(self, src_record_field_name, olap_schema_dimension):
        self._dimensions[src_record_field_name] = olap_schema_dimension


    def map_src_record_field_to_non_dimension(self, src_record_field_name, fact_field_name, fact_field_type):
        nd_field = NonDimensionField(fact_field_name, fact_field_type)
        self._direct_mappings[src_record_field_name] = nd_field


    def get_fact_values(self, source_record, **kwargs):
        data = {}
        #print('### source record info: %s'%  source_record)

        for src_record_field_name in self._direct_mappings.keys():
            non_dimension_field = self._direct_mappings[src_record_field_name]
            data[non_dimension_field.field_name] = source_record[src_record_field_name]

        for src_record_field_name in self._dimensions.keys():
            dimension = self._dimensions[src_record_field_name]
            data[dimension.fact_table_field_name] = dimension.lookup_id_for_value(source_record[src_record_field_name],
                                                                                  **kwargs)

        return data



class OLAPSchemaMappingContextBuilder(object):
    def __init__(self, yaml_config_filename, **kwargs):
        kwreader = common.KeywordArgReader('context_name')
        kwreader.read(**kwargs)
        self._context_name = kwreader.get_value('context_name')
        self._yaml_config = None
        with open(yaml_config_filename, 'r') as f:
            self._yaml_config = yaml.load(f)


    def load_sqltype_class(self, classname, **kwargs):
        pk_type_module = self._yaml_config['globals']['sql_datatype_module']
        klass = common.load_class(classname, pk_type_module)
        return klass


    def load_fact_pk_sqltype_class(self, classname, **kwargs):
        pk_type_module = self._yaml_config['globals']['primary_key_datatype_module']
        klass = common.load_class(classname, pk_type_module)
        return klass


    def load_fact_pk_type_options(self):
        #TODO: pull this from the YAML file
        return {'binary': False}


    def build(self):
        fact_config = self._yaml_config['mappings'][self._context_name]['fact']
        fact_table_name = fact_config['table_name']
        fact_pk_field_name = fact_config['primary_key_name']
        fact_pk_field_classname = fact_config['primary_key_type']


        fact_pk_field_class = self.load_fact_pk_sqltype_class(fact_pk_field_classname)
        pk_type_options = self.load_fact_pk_type_options()

        fact = OLAPSchemaFact(fact_table_name,
                              fact_pk_field_name,
                              fact_pk_field_class(**pk_type_options))

        mapping_context = OLAPSchemaMappingContext(fact)

        for dim_name in self._yaml_config['mappings'][self._context_name]['dimensions']:
            dim_config = self._yaml_config['mappings'][self._context_name]['dimensions'][dim_name]

            pk_type_class = self.load_sqltype_class(dim_config['primary_key_type'])

            dim = OLAPSchemaDimension(fact_table_field_name=dim_config['fact_table_field_name'],
                                      dim_table_name=dim_config['table_name'],
                                      key_field_name=dim_config['primary_key_field_name'],
                                      value_field_name=dim_config['value_field_name'],
                                      primary_key_type=pk_type_class(),
                                      id_lookup_function=dimension_id_lookup_func)

            source_record_fieldname = dim_name
            mapping_context.map_src_record_field_to_dimension(source_record_fieldname, dim)


        for non_dim_name in self._yaml_config['mappings'][self._context_name]['non_dimensions']:
            non_dim_config = self._yaml_config['mappings'][self._context_name]['non_dimensions'][non_dim_name]
            source_record_field = non_dim_name
            fact_field_name = non_dim_config['fact_field_name']
            fact_field_type = non_dim_config['fact_field_type']
            mapping_context.map_src_record_field_to_non_dimension(source_record_field,
                                                                  fact_field_name,
                                                                  self.load_sqltype_class(fact_field_type))

        return mapping_context
            

class OLAPSchemaDDLGenerator(object):
    fact_ddl_template = '''
    CREATE TABLE {schema}.{fact_table_name}
    (
        {pk_field_name} {pk_field_type} PRIMARY KEY NOT NULL,    
        {fields}    
    );
    '''

    dimension_ddl_template = '''
    CREATE TABLE {schema}.{dim_table_name}
    (
        {pk_field_name} {pk_field_type} PRIMARY KEY NOT NULL,
        {value_field_name} {value_field_type} NOT NULL
    )
    '''
    
    
    def __init__(self, schema_mapping_context):
        self._context = schema_mapping_context
        self._schema = 'test'
        

    def generate_fact_table_ddl(self):
        field_defs = []
        for field_name in self._context.dimension_names:
            field_def = '%s %s NOT NULL' % (field_name, self._context.sqltype_for_field(field_name))
            field_defs.append(field)

        for field_name in self._context.non_dimension_names:
            field_def = '%s %s NOT NULL' % (field_name, self._context.sqltype_for_field(field_name))
            
        field_def_block = ',\n'.join(field_defs)
        sql = fact_ddl_template.format(schema=self._schema,
                                      fact_table_name=self._context.fact.table_name,
                                      pk_field_name=self._context.fact.primary_key_field_name,
                                      fields=field_def_block)
        return sql


    def generate_dimension_table_ddl(self, dimension_name):
        dim = self._context.get_dimension(dimension_name)
        
        sql = dimension_ddl_template.format(schema=self._schema,
                                            pk_field_name=dim.primary_key_field_name,
                                            pk_field_type=self.sqltype(dim.primary_key_field_type),
                                            value_field_name=dim.value_field_name,
                                            value_field_type=self.sqltype(dim.value_field_type))
        return sql
        


class OLAPStarSchemaRelay(DataRelay):
    def __init__(self, persistence_mgr, olap_schema_map_ctx, **kwargs):
        DataRelay.__init__(self, **kwargs)
        self._pmgr = persistence_mgr
        self._dbconnection = self._pmgr.database.engine.connect()
        self._schema_mapping_context = olap_schema_map_ctx

        fact_record_type_builder = sqlx.SQLDataTypeBuilder('FactRecord', self._schema_mapping_context.fact.table_name)
        fact_record_type_builder.add_primary_key_field(self._schema_mapping_context.fact.primary_key_field_name,
                                                       self._schema_mapping_context.fact.primary_key_field_type)

        for name in self._schema_mapping_context.dimension_names:
            fact_record_type_builder.add_field(name,
                                               self._schema_mapping_context.get_dimension(name).fact_table_field_name,
                                               self._schema_mapping_context.get_dimension(name).primary_key_field_type)

        for name in self._schema_mapping_context.non_dimension_names:
            nd_field = self._schema_mapping_context.get_non_dimension_field(name)
            fact_record_type_builder.add_field(nd_field.field_name,
                                               nd_field.field_type)

        self._FactRecordType = fact_record_type_builder.build()


    def _handle_checkpoint_event(self, **kwargs):
        pass


    def _send(self, msg_header, kafka_message, **kwargs):
        log.debug("writing kafka log message to db...")
        log.debug('### kafka_message keys: %s' % '\n'.join(kafka_message.keys()))
        outbound_record = {}
        fact_data = self._schema_mapping_context.get_fact_values(kafka_message.get('body'), 
                                                                 persistence_manager=self._pmgr)

        print('### OLAP fact data:')
        print(common.jsonpretty(fact_data))

        insert_query_template = '''
        INSERT INTO {fact_table} ({field_names})
        VALUES ({data_placeholders});
        '''

        data_placeholder_segment = ', '.join([':%s' % name for name in fact_data.keys()])

        print('### initial rendering of insert statement: ')
        iqtemplate_render = insert_query_template.format(fact_table=self._schema_mapping_context.fact.table_name,
                                                         field_names=','.join(fact_data.keys()),
                                                         data_placeholders=data_placeholder_segment)
        print(iqtemplate_render)

        insert_statement = text(iqtemplate_render)
        insert_statement = insert_statement.bindparams(**fact_data)                                                     
        #dbconnection = self._pmgr.database.engine.connect()
        result = self._dbconnection.execute(insert_statement)


