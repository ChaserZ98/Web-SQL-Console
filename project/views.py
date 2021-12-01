import traceback

import sqlparse
from django.shortcuts import render
# from django.http import HttpResponse
from django.http import JsonResponse
# import mysql.connector as mysql
import time
from django.db import connections
import re
from project import mongoClient
from datetime import datetime

# def index(request):
#     return HttpResponse('project index page')


# connect to mysql by default when the project index page is requested for the first time
def index(request):
    """
    :param request: http request
    :return: index web page with data
    """
    return render(request, 'index.html')


def connectToDB(request):
    """
    :param request: http request
    :return: schema/database name
    """
    databaseType = request.POST.get('databaseType')
    responseStatus = 0  # 0 for no error, 1 for error
    cursor = None
    db = None
    try:
        if databaseType == "mongodb":
            db = mongoClient["instacart"]
        else:
            cursor = connections[databaseType].cursor()
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})
    result = ''
    try:
        if databaseType == 'mysql':
            cursor.execute('select database()')
            result = cursor.fetchone()[0]
        elif databaseType == 'redshift':
            cursor.execute('select current_database()')
            result = cursor.fetchone()[0]
        elif databaseType == 'mongodb':
            result = db.name
            # temp = eval("db.instacart_fact_table.find().limit(1)")
            # for doc in temp:
            #     print(doc)
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    return JsonResponse({'currentDatabase': result})


def checkQuery(query, row, rowPerPage):
    # since show, describe doesn't support limit, then only when using select command will we append limit and offset
    if str(query[:6]).lower() == 'select':
        # In redshift (postgreSQL), top and limit is not allowed to use together.
        query = query.replace(';', '')
        top = re.search('\s+top\s+(\d+)', query, flags=re.IGNORECASE)
        limitOffset = re.search('\s+limit\s+(\d+)\s*,\s+(\d+)', query, flags=re.IGNORECASE)
        limit = re.search('\s+limit\s+(\d+)', query, flags=re.IGNORECASE)
        offset = re.search('\s+offset\s+(\d+)', query, flags=re.IGNORECASE)
        if top:
            limitNum = int(top.group(1))
            query = re.sub('(\s+top\s+\d+)', '', query, flags=re.IGNORECASE)
            if offset:
                query = re.sub('(\s+offset\s+\d+)', '', query, flags=re.IGNORECASE)
                offsetNum = int(offset.group(1))
            if rowPerPage + row > limitNum:
                query += " limit %s offset %s" % (limitNum - row, row if not offset else row + offsetNum)
            else:
                query += " limit %s offset %s" % (rowPerPage, row if not offset else row + offsetNum)
        elif limitOffset:
            offsetNum = int(limitOffset.group(1))
            limitNum = int(limitOffset.group(2))
            query = re.sub('(\s+limit\s+\d+\s*,\s+\d+)', '', query, flags=re.IGNORECASE)
            if rowPerPage + row > limitNum:
                query += " limit %s offset %s" % (limitNum - row, row + offsetNum)
            else:
                query += " limit %s offset %s" % (rowPerPage, row + offsetNum)
        elif limit:
            limitNum = int(limit.group(1))
            query = re.sub('(\s+limit\s+\d+)', '', query, flags=re.IGNORECASE)
            if offset:
                query = re.sub('(\s+offset\s+\d+)', '', query, flags=re.IGNORECASE)
                offsetNum = int(offset.group(1))
            if rowPerPage + row > limitNum:
                query += " limit %s offset %s" % (limitNum - row, row if not offset else row + offsetNum)
            else:
                query += " limit %s offset %s" % (rowPerPage, row if not offset else row + offsetNum)
        elif offset:
            query = re.sub('(\s+offset\s+\d+)', '', query, flags=re.IGNORECASE)
            offsetNum = int(offset.group(1))
            row += offsetNum
            query += " limit %s offset %s" % (rowPerPage, row)
        else:
            query += " limit %s offset %s" % (rowPerPage, row)
    return query


def updateData(request):
    databaseType = request.POST.get('databaseType')
    draw = request.POST.get('draw')
    row = int(request.POST.get('start'))
    rowPerPage = int(request.POST.get('length'))
    attribute = request.POST.getlist('attribute[]')
    query = request.POST.get('query')
    totalRecords = request.POST.get('totalRecords')
    print(f"row: {row}")
    print(f"rowPerPage: {rowPerPage}")

    try:
        query = checkQuery(query, row, rowPerPage)
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    print(f"Executed Query: {query}")

    try:
        cursor = connections[databaseType].cursor()
        recordsFiltered = cursor.execute(query)
        result = cursor.fetchall()
        data = []
        for row in result:
            data.append({"\'" + attribute[i] + "\'": str(row[i]) for i in range(len(attribute))})
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    print(f"draw: {draw}")
    print(f"recordsTotal: {totalRecords}")
    print(f"recordsFiltered: {totalRecords}")
    print(f"data: {data}")

    return JsonResponse({
        'draw': draw,
        'recordsTotal': totalRecords,
        'recordsFiltered': totalRecords,
        'data': data})


# receive ajax request and return requested data
def ajax(request):
    """
    :param request: http request
    :return: json response including query result
    """
    # received data
    databaseType = request.POST.get('databaseType')  # mysql/redshift/mongoDB
    currentDatabase = request.POST.get('currentDatabase')  # used database/schema
    query = str(request.POST.get('query'))  # input sql query
    query = sqlparse.format(query, reindent=True, keyword_case='upper')
    query = sqlparse.split(query)

    # query = [sql.replace('\n', ' ').strip() for sql in query.split(';') if sql]
    print('Received Query: ', query)

    # send data
    tableQuery = query          # query that generates each table
    displayedQuery = []         # query that will be displayed in result section
    resultAttribute = []        # table attribute
    queryResult = []            # table data
    totalRecords = []           # numeric value of number of rows influenced by a sql query
    executedTimeStamp = []      # string value of timestamp when each query is executed
    influencedRowResult = []    # string value of number of rows influenced by a sql query
    executionTime = []          # string value of execution time of each query
    totalExecutionTime = 0      # total time of executing all sql queries
    # currentDatabase           # return the current database/schema back in case it changed

    cursor = None
    db = None
    try:
        if databaseType == 'mongodb':
            db = mongoClient[currentDatabase]
        else:
            cursor = connections[databaseType].cursor()
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    for sql in query:
        startTime = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            responseStatus = 1
            traceback.print_exc()
            return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})
        endTime = time.time()
        totalExecutionTime += endTime - startTime

        executionTime.append(str(round(endTime - startTime, 2)) + ' s')
        influencedRow = cursor.rowcount
        totalRecords.append(influencedRow)
        executedTimeStamp.append(datetime.now().strftime("%m/%d/%Y %H:%M:%S"))

        try:
            # change the color of keywords and identifier in the query
            colorQuery = ""
            keywordColor = "rgb(43, 51, 179)"
            identifierColor = "rgb(134, 179, 0)"
            for token in sqlparse.parse(sql)[0].flatten():
                if token.ttype in sqlparse.tokens.Keyword or token.ttype in sqlparse.tokens.Punctuation:
                    colorQuery += f"<span style='color: {keywordColor}'>{token.value}</span>"
                elif token.ttype in sqlparse.tokens.Name or token.ttype in sqlparse.tokens.Wildcard:
                    colorQuery += f"<span style='color: {identifierColor}'>{token.value}</span>"
                elif token.ttype is sqlparse.tokens.Whitespace:
                    colorQuery += '&nbsp;'
                else:
                    colorQuery += token.value
            colorQuery = colorQuery.replace("\n", "</br>")
            displayedQuery.append(colorQuery)

            # DML (INSERT, UPDATE) will use "affected", DQL (SELECT) will use "retrieved"
            if influencedRow == 0 or influencedRow == -1:
                influencedRowResult.append(None)
            elif influencedRow == 1:
                temp = f"{influencedRow} row "
                temp += "retrieved" if cursor.description else "affected"
                influencedRowResult.append(temp)
            else:
                temp = f"{influencedRow} rows "
                temp += "retrieved" if cursor.description else "affected"
                influencedRowResult.append(temp)
        except Exception as e:
            responseStatus = 1
            traceback.print_exc()
            return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})
        try:
            if databaseType == 'mysql':
                queryResult.append(cursor.fetchmany(size=100))
            elif databaseType == 'redshift':
                # no result will be fetched if the last query is DML (INSERT, UPDATE)
                if cursor.description:
                    # if cursor.description is not None, then it means the last query is DQL (SELECT)
                    queryResult.append(cursor.fetchmany(size=100))
                else:
                    queryResult.append(())
        except Exception as e:
            responseStatus = 1
            traceback.print_exc()
            return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})
        try:
            if cursor.description:
                resultAttribute.append([attribute[0] for attribute in cursor.description])
            else:
                resultAttribute.append(None)
        except Exception as e:
            responseStatus = 1
            traceback.print_exc()
            return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    try:
        for i in range(len(influencedRowResult)):
            print(f'Executed Query: {query[i]}')
            print(f'Displayed Query: {displayedQuery[i]}')
            print(f'Influenced Row: {influencedRowResult[i]}')
            print(f'Result Attributes: {resultAttribute[i]}')
            print(f'Query Result: {queryResult[i][:100]}')  # limit query result to 100 rows
            print()
        print(f'Total Execution Time: {round(totalExecutionTime, 2)} s')
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    try:
        if databaseType == 'mysql':
            cursor.execute('select database()')
            currentDatabase = cursor.fetchone()[0]
        elif databaseType == 'redshift':
            cursor.execute('select current_database()')
            currentDatabase = cursor.fetchone()[0]
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    return JsonResponse(
        {'tableQuery': tableQuery,
         'displayedQuery': displayedQuery,
         'totalRecords': totalRecords,
         'executedTimeStamp': executedTimeStamp,
         'influencedRowResult': influencedRowResult,
         'resultAttribute': resultAttribute,
         'queryResult': queryResult,
         'executionTime': executionTime,
         'totalExecutionTime': str(round(totalExecutionTime, 2)) + ' s',
         'currentDatabase': str(currentDatabase)})


# def sqlToMongo(query):
#     if str(query[:6]).lower() == 'select':


def testMongo(request):
    databaseType = request.POST.get('databaseType')
    responseStatus = 0  # 0 for no error, 1 for error
    db = None
    try:
        db = mongoClient["instacart"]
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})
    result = ''
    query = "db[\"instacart_fact_table\"].find().limit(1)"
    cur = eval(query)
    # cur = db["instacart_fact_table"].find().limit(1)
    for doc in cur:
        print(doc)
    try:
        result = db.name
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    return JsonResponse({'currentDatabase': result})
