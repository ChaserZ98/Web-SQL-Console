from mo_sql_parsing import parse

AGGREGATE_FUNCTIONS = ['max', 'min', 'count', 'avg', 'sum']
SCALAR_FUNCTIONS = ['sqrt']

BOOLEAN_OPERATORS = ['and', 'not', 'or']
COMPARISON_OPERATORS = ['eq', 'gt', 'gte', 'lt', 'lte', 'ne']
NULL_OPERATORS = ['exists', 'missing']  # exists → is not null, missing → is null
STRING_OPERATORS = ['like', 'not_like']

# select field type
LITERAL = 'LITERAL'
AGGREGATE_FUNCTION = 'AGGREGATE_FUNCTION'
SCALAR_FUNCTION = 'SCALAR_FUNCTION'
COLUMN_NAME = 'COLUMN_NAME'
WILD_CARD = '*'


def getSelectFieldType(field):
    if type(field) is dict:
        # pure column name
        if type(field['value']) is str:
            return COLUMN_NAME
        # literal
        if type(field['value']) is int or field['value'].get('literal'):
            return LITERAL
        # aggregate function
        for agg in AGGREGATE_FUNCTIONS:
            if agg in field['value']:
                return AGGREGATE_FUNCTION
        # scalar function
        for scalar in SCALAR_FUNCTIONS:
            if scalar in field['value']:
                return SCALAR_FUNCTION
        return 'NOT SPECIFIED YET'
    # wild card
    elif field == '*':
        return WILD_CARD
    else:
        return 'NOT SPECIFIED YET'


def getSelectFieldTypesDic(fields):
    containWildCard = False  # check for wildcard (*)
    containPureColumnName = False  # check for column name
    containLiteral = False  # check for literal, e.g. 1 or 'a'
    containAggregateFunc = False  # check for aggregate function
    containScalarFunc = False  # check for scalar function

    # multiple fields (assuming no wildcard)
    if type(fields) is list:
        for field in fields:
            fieldType = getSelectFieldType(field)
            if fieldType == LITERAL:
                containLiteral = True
            elif fieldType == AGGREGATE_FUNCTION:
                containAggregateFunc = True
            elif fieldType == SCALAR_FUNCTION:
                containScalarFunc = True
            elif fieldType == COLUMN_NAME:
                containPureColumnName = True
    # single field (including wildcard)
    elif type(fields) is dict or fields == '*':
        fieldType = getSelectFieldType(fields)
        if fieldType == LITERAL:
            containLiteral = True
        elif fieldType == AGGREGATE_FUNCTION:
            containAggregateFunc = True
        elif fieldType == SCALAR_FUNCTION:
            containScalarFunc = True
        elif fieldType == COLUMN_NAME:
            containPureColumnName = True
        elif fieldType == WILD_CARD:
            containWildCard = True

    return {
        'containWildCard': containWildCard,
        'containPureColumnName': containPureColumnName,
        'containLiteral': containLiteral,
        'containAggregateFunc': containAggregateFunc,
        'containScalarFunc': containScalarFunc
    }


# select
def parseOneSelectField(field, selectFieldTypesDict, groupbyColumns, group, project):
    # literal
    literal = field['value'].get('literal') if type(field['value']) is dict else None

    # aggregate function
    aggregateFunc = None
    if selectFieldTypesDict['containAggregateFunc']:
        for agg in AGGREGATE_FUNCTIONS:
            if agg in field['value']:
                aggregateFunc = agg
                break

    # scalar function
    scalarFunc = None
    if selectFieldTypesDict['containScalarFunc']:
        for scalar in SCALAR_FUNCTIONS:
            if scalar in field['value']:
                scalarFunc = scalar
                break

    # literal case
    if type(field['value']) is dict and literal:
        alias = field['name'] if field.get('name') else literal
        project[alias] = {'$literal': literal}
    # aggregate function case
    elif type(field['value']) is dict and aggregateFunc:
        column = field['value'][aggregateFunc]
        alias = field['name'] if field.get('name') else f"{aggregateFunc}({column})"
        mongoAccumulator = None
        mongoExpression = None

        if '_id' not in group:
            group['_id'] = None
        if aggregateFunc == 'sum':
            mongoAccumulator = "$sum"
            mongoExpression = f"${column}"
            # group[f"{aggregateFunc}({column})"] = {"$sum": f"${column}"}
        elif aggregateFunc == 'count':
            mongoAccumulator = "$sum"
            mongoExpression = 1
            # group[f"{aggregateFunc}({column})"] = {"$sum": 1}
        elif aggregateFunc == 'avg':
            mongoAccumulator = "$avg"
            mongoExpression = f"${column}"
            # group[f"{aggregateFunc}({column})"] = {"$avg": f"${column}"}
        elif aggregateFunc == 'max':
            mongoAccumulator = "$max"
            mongoExpression = f"${column}"
            # group[f"{aggregateFunc}({column})"] = {"$max": f"${column}"}
        elif aggregateFunc == 'min':
            mongoAccumulator = "$min"
            mongoExpression = f"${column}"
            # group[f"{aggregateFunc}({column})"] = {"$min": f"${column}"}
        group[f"{aggregateFunc}({column})"] = {mongoAccumulator: mongoExpression}

        project[alias] = f"${aggregateFunc}({column})"
        project['_id'] = 0
    # scalar function case
    elif type(field['value']) is dict and scalarFunc:
        column = field['value'][scalarFunc]
        alias = field['name'] if field.get('name') else f"{scalarFunc}({column})"
        projectExpression = {f'${scalarFunc}': f'${column}'}
        if selectFieldTypesDict['containAggregateFunc'] or groupbyColumns:
            if alias not in groupbyColumns:
                group[f"{scalarFunc}({column})"] = {'$first': projectExpression}
                project[alias] = f'${scalarFunc}({column})'
            else:
                project[alias] = f'$_id.{scalarFunc}({column})'
        else:
            project[alias] = projectExpression
        project['_id'] = 0
    # pure column name case
    else:
        column = field['value']
        alias = field['name'] if field.get('name') else column
        if selectFieldTypesDict['containAggregateFunc'] or groupbyColumns:
            if alias not in groupbyColumns:
                group[column] = {'$first': f'${column}'}
                project[alias] = f"${column}"
            else:
                project[alias] = f"$_id.{column}"
        else:
            project[alias] = f"${column}"
        project['_id'] = 0


def parseSelectFields(fields, groupbyColumns, group, project):
    # print(fields)
    selectFieldTypesDict = getSelectFieldTypesDic(fields)

    # multiple field (assume no wildcard)
    if type(fields) is list:
        for field in fields:
            parseOneSelectField(field, selectFieldTypesDict, groupbyColumns, group, project)
    # single field (assume no wildcard)
    elif type(fields) is dict:
        parseOneSelectField(fields, selectFieldTypesDict, groupbyColumns, group, project)
    # wildcard *
    # else:
    #     containWildCard = True


# select distinct
def parseOneSelectDistinctField(field, selectFieldTypesDict, groupbyColumns, group, project):
    # literal
    literal = field['value'].get('literal') if type(field['value']) is dict else None

    # aggregate function
    aggregateFunc = None
    if selectFieldTypesDict['containAggregateFunc']:
        for agg in AGGREGATE_FUNCTIONS:
            if agg in field['value']:
                aggregateFunc = agg
                break

    # scalar function
    scalarFunc = None
    if selectFieldTypesDict['containScalarFunc']:
        for scalar in SCALAR_FUNCTIONS:
            if scalar in field['value']:
                scalarFunc = scalar
                break

    # literal case
    if type(field['value']) is dict and literal:
        alias = field['name'] if field.get('name') else literal
        if selectFieldTypesDict['containAggregateFunc']:
            group['_id'] = None
            group[literal] = {'$first': {'$literal': literal}}
            project[alias] = f'${literal}'
        else:
            if '_id' not in group:
                group['_id'] = {literal: {'$literal': literal}}
            else:
                group['_id'][literal] = {'$literal': literal}
            project[alias] = f'$_id.{literal}'
        project['_id'] = 0
    # aggregate function case
    elif type(field['value']) is dict and aggregateFunc:
        column = field['value'][aggregateFunc]
        alias = field['name'] if field.get('name') else f"{aggregateFunc}({column})"
        mongoAccumulator = None
        mongoExpression = None

        if '_id' not in group:
            group['_id'] = None
        if aggregateFunc == 'sum':
            mongoAccumulator = "$sum"
            mongoExpression = f"${column}"
        elif aggregateFunc == 'count':
            mongoAccumulator = "$sum"
            mongoExpression = 1
        elif aggregateFunc == 'avg':
            mongoAccumulator = "$avg"
            mongoExpression = f"${column}"
        elif aggregateFunc == 'max':
            mongoAccumulator = "$max"
            mongoExpression = f"${column}"
        elif aggregateFunc == 'min':
            mongoAccumulator = "$min"
            mongoExpression = f"${column}"
        group[f"{aggregateFunc}({column})"] = {mongoAccumulator: mongoExpression}

        project[alias] = f"${aggregateFunc}({column})"
        project['_id'] = 0
    # scalar function case
    elif type(field['value']) is dict and scalarFunc:
        column = field['value'][scalarFunc]
        alias = field['name'] if field.get('name') else f"{scalarFunc}({column})"
        projectExpression = {f'${scalarFunc}': f'${column}'}

        if selectFieldTypesDict['containAggregateFunc']:
            group['_id'] = None
            group[f'{scalarFunc}({column})'] = {'$first': projectExpression}
            project[alias] = f'${scalarFunc}({column})'
        else:
            if '_id' not in group:
                group['_id'] = {f'{scalarFunc}({column})': projectExpression}
            else:
                group['_id'][f'{scalarFunc}({column})'] = projectExpression
            project[alias] = f'$_id.{scalarFunc}({column})'
        project['_id'] = 0
    # pure column name case
    else:
        column = field['value']
        alias = field['name'] if field.get('name') else column
        if selectFieldTypesDict['containAggregateFunc']:
            group['_id'] = None
            group[column] = {'$first': f'${column}'}
            project[alias] = f'${column}'
        else:
            if '_id' not in group:
                group['_id'] = {column: f'${column}'}
            else:
                group['_id'][column] = f'${column}'
            project[alias] = f'$_id.{column}'
        project['_id'] = 0


def parseSelectDistinctFields(fields, groupbyColumns, group, project):
    # print(fields)
    selectFieldTypesDict = getSelectFieldTypesDic(fields)

    # multiple field (assume no wildcard)
    if type(fields) is list:
        for field in fields:
            parseOneSelectDistinctField(field, selectFieldTypesDict, groupbyColumns, group, project)
    # single field (assume no wildcard)
    elif type(fields) is dict:
        parseOneSelectDistinctField(fields, selectFieldTypesDict, groupbyColumns, group, project)
    # wildcard *
    # else:
    #     containWildCard = True


# where
def recursiveParseWhere(fields):
    # print(f'fields: {fields}')
    if type(fields) is not dict:
        return fields
    operator = None
    booleanOperator = False
    comparisonOperator = False
    nullOperator = False
    stringOperator = False

    for bo in BOOLEAN_OPERATORS:
        if bo in fields:
            booleanOperator = True
            operator = bo
            break
    if not operator:
        for co in COMPARISON_OPERATORS:
            if co in fields:
                comparisonOperator = True
                operator = co
                break
    if not operator:
        for no in NULL_OPERATORS:
            if no in fields:
                nullOperator = True
                operator = no
                break
    if not operator:
        for so in STRING_OPERATORS:
            if so in fields:
                stringOperator = True
                operator = so
                break
    print(f'operator: {operator}, booleanOperator: {booleanOperator}, comparisonOperator: {comparisonOperator}, nullOperator: {nullOperator}, stringOperator: {stringOperator}')
    expression = {}
    if operator == 'not':
        expression[f'${operator}'] = [recursiveParseWhere(fields[operator])]
    elif operator == 'missing':
        expression[fields[operator]] = {'$eq': None}
        # expression['$eq'] = [f'${recursiveParseWhere(fields[operator])}', None]
    elif operator == 'exists':
        expression[fields[operator]] = {'$ne': None}
        # expression['$ne'] = [f'${recursiveParseWhere(fields[operator])}', None]
    elif operator == 'like':
        column = fields[operator][0]
        regex = fields[operator][1]['literal']
        regex = regex.replace('%', '.*')
        expression[column] = {'$regex': regex}
    elif operator == 'not_like':
        column = fields[operator][0]
        regex = fields[operator][1]['literal']
        regex = regex.replace('%', '.*')
        expression[column] = {'$not': {'$regex': regex}}
    else:
        firstElement = recursiveParseWhere(fields[operator][0])
        secondElement = recursiveParseWhere(fields[operator][1])
        if booleanOperator:
            expression[f'${operator}'] = [firstElement, secondElement]
        else:
            expression[firstElement] = {f'${operator}': secondElement}
            # expression[f'${operator}'] = [f'${firstElement}', secondElement]
    return expression
    # if comparisonOperator:
    #     recursiveParseWhere(fields[comparisonOperator][0])
    #     recursiveParseWhere(fields[comparisonOperator][1])
    # elif booleanOperator:
    #     recursiveParseWhere(fields[booleanOperator][0])
    #     recursiveParseWhere(fields[booleanOperator][1])


def parseOrderByFields(fields, sort):
    # multiple fields
    if type(fields) is list:
        for field in fields:
            column = field.get('value')
            desc = field.get('sort')
            print(column)
            if desc:
                sort[column] = -1
            else:
                sort[column] = 1
    # single field
    else:
        column = fields.get('value')
        desc = fields.get('sort')
        if desc:
            sort[column] = -1
        else:
            sort[column] = 1


def parseOneGroupByField(field, group):
    column = field.get('value')
    if '_id' in group:
        if group['_id']:
            group['_id'][column] = f'${column}'
        else:
            group['_id'] = {column: f'${column}'}
    else:
        group['_id'] = {column: f'${column}'}


def parseGroupByFields(fields, group):
    print(fields)
    # multiple fields
    if type(fields) is list:
        for field in fields:
            parseOneGroupByField(field, group)
    else:
        parseOneGroupByField(fields, group)


# todo
def recursiveParseHaving(fields):
    # print(f'fields: {fields}')
    if type(fields) is not dict:
        return fields
    operator = None
    booleanOperator = False
    comparisonOperator = False
    nullOperator = False
    stringOperator = False

    for bo in BOOLEAN_OPERATORS:
        if bo in fields:
            booleanOperator = True
            operator = bo
            break
    if not operator:
        for co in COMPARISON_OPERATORS:
            if co in fields:
                comparisonOperator = True
                operator = co
                break
    if not operator:
        for no in NULL_OPERATORS:
            if no in fields:
                nullOperator = True
                operator = no
                break
    if not operator:
        for so in STRING_OPERATORS:
            if so in fields:
                stringOperator = True
                operator = so
                break
    print(
        f'operator: {operator}, booleanOperator: {booleanOperator}, comparisonOperator: {comparisonOperator}, nullOperator: {nullOperator}, stringOperator: {stringOperator}')
    expression = {}
    if operator == 'not':
        expression[f'${operator}'] = [recursiveParseWhere(fields[operator])]
    elif operator == 'missing':
        expression[fields[operator]] = {'$eq': None}
        # expression['$eq'] = [f'${recursiveParseWhere(fields[operator])}', None]
    elif operator == 'exists':
        expression[fields[operator]] = {'$ne': None}
        # expression['$ne'] = [f'${recursiveParseWhere(fields[operator])}', None]
    elif operator == 'like':
        column = fields[operator][0]
        regex = fields[operator][1]['literal']
        regex = regex.replace('%', '.*')
        expression[column] = {'$regex': regex}
    elif operator == 'not_like':
        column = fields[operator][0]
        regex = fields[operator][1]['literal']
        regex = regex.replace('%', '.*')
        expression[column] = {'$not': {'$regex': regex}}
    else:
        firstElement = recursiveParseWhere(fields[operator][0])
        secondElement = recursiveParseWhere(fields[operator][1])
        if booleanOperator:
            expression[f'${operator}'] = [firstElement, secondElement]
        else:
            expression[firstElement] = {f'${operator}': secondElement}
            # expression[f'${operator}'] = [f'${firstElement}', secondElement]
    return expression


def convertSelect(tokens):
    selectFields = tokens['select'] if 'select' in tokens else None
    selectDistinctFields = tokens['select_distinct'] if 'select_distinct' in tokens else None

    fromField = tokens['from']
    whereFields = tokens['where'] if 'where' in tokens else None
    groupbyFields = tokens['groupby'] if 'groupby' in tokens else None
    havingFields = tokens['having'] if 'having' in tokens else None
    orderbyFields = tokens['orderby'] if 'orderby' in tokens else None
    limitField = tokens['limit'] if 'limit' in tokens else None
    offsetField = tokens['offset'] if 'offset' in tokens else None

    print(f"select: {selectFields}")
    print(f"select_distinct: {selectDistinctFields}")
    print(f"from: {fromField}")
    print(f"where: {whereFields}")
    print(f"groupby: {groupbyFields}")
    print(f"having: {havingFields}")
    print(f"orderby: {orderbyFields}")
    print(f"limit: {limitField}")
    print(f"offset: {offsetField}")

    pipeline = []
    group = {}
    project = {}
    match_where = {}
    sort = {}
    match_having = {}

    groupbyColumns = []
    if groupbyFields:
        if type(groupbyFields) is list:
            for field in groupbyFields:
                groupbyColumns.append(field['value'])
        else:
            groupbyColumns.append(groupbyFields['value'])
    print(f'groupbyColumns: {groupbyColumns}')

    # select fields
    if selectFields:
        parseSelectFields(selectFields, groupbyColumns, group, project)
    # select distinct
    elif selectDistinctFields:
        parseSelectDistinctFields(selectDistinctFields, groupbyColumns, group, project)
    if groupbyFields:
        parseGroupByFields(groupbyFields, group)
    if whereFields:
        match_where = recursiveParseWhere(whereFields)
        # match = parseWhereFields(whereFields, match)
    if havingFields:
        match_having = recursiveParseHaving(havingFields)
    if orderbyFields:
        parseOrderByFields(orderbyFields, sort)

    if match_where:
        pipeline.append({'$match': match_where})
    if group:
        pipeline.append({'$group': group})
    if match_having:
        pipeline.append({'$match': match_having})
    if project:
        pipeline.append({'$project': project})
    if sort:
        pipeline.append({'$sort': sort})
    if offsetField:
        pipeline.append({'$skip': offsetField})
    if limitField:
        pipeline.append({"$limit": limitField})

    print()
    print(f"pipeline: {pipeline}")

    mongoQuery = f"db.{fromField}.aggregate({pipeline}, {{allowDiskUse: True}})"
    print(f"MongoDB Query: {mongoQuery}")

    return mongoQuery


def sql2MongoShell(tokens):
    queryType = list(tokens.keys())[0]
    if queryType == 'select' or queryType == 'select_distinct':
        convertSelect(tokens)
    return


if __name__ == '__main__':
    sql = "select add_to_cart_order from instacart_fact_table where add_to_cart_order > 10 group by add_to_cart_order"
    # sql = "Select price as price from instacart_fact_table limit 1 offset 2;"
    tokens = parse(sql)
    # tokens = parse("use adni")
    # print(list(tokens.keys())[0])
    # for key, value in tokens.items():
    # 	print(key, value)
    # print()
    print(tokens)
    print()
    sql2MongoShell(tokens)
    print(f"SQL: {sql}")
