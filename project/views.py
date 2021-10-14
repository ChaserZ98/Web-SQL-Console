import traceback

from django.shortcuts import render
# from django.http import HttpResponse
from django.http import JsonResponse
# import mysql.connector as mysql
import time
from django.db import connections
import re


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
    responseStatus = 0      # 0 for no error, 1 for error
    cursor = None
    try:
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
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    return JsonResponse({'currentDatabase': result})


def checkQuery(query, row, rowPerPage):
    # since show, describe doesn't support limit, then only when using select command will we append limit and offset
    if str(query[:6]).lower() == 'select':
        # In redshift (postgreSQL), top and limit is not allowed to use together.
        top = re.search('\s+top\s+(\d+)', query, re.IGNORECASE)
        limitOffset = re.search('\s+limit\s+(\d+)\s*,\s+(\d+)', query, re.IGNORECASE)
        limit = re.search('\s+limit\s+(\d+)', query, re.IGNORECASE)
        offset = re.search('\s+offset\s+(\d+)', query, re.IGNORECASE)
        if top:
            limitNum = int(top.group(1))
            query = re.sub('(\s+top\s+\d+)', '', query, re.IGNORECASE)
            if offset:
                query = re.sub('(\s+offset\s+\d+)', '', query, re.IGNORECASE)
                offsetNum = int(offset.group(1))
            if rowPerPage + row > limitNum:
                query += " limit %s offset %s" % (limitNum - row, row if not offset else row + offsetNum)
            else:
                query += " limit %s offset %s" % (rowPerPage, row if not offset else row + offsetNum)
        elif limitOffset:
            offsetNum = int(limitOffset.group(1))
            limitNum = int(limitOffset.group(2))
            query = re.sub('(\s+limit\s+\d+\s*,\s+\d+)', '', query, re.IGNORECASE)
            if rowPerPage + row > limitNum:
                query += " limit %s offset %s" % (limitNum - row, row + offsetNum)
            else:
                query += " limit %s offset %s" % (rowPerPage, row + offsetNum)
        elif limit:
            limitNum = int(limit.group(1))
            query = re.sub('(\s+limit\s+\d+)', '', query, re.IGNORECASE)
            if offset:
                query = re.sub('(\s+offset\s+\d+)', '', query, re.IGNORECASE)
                offsetNum = int(offset.group(1))
            if rowPerPage + row > limitNum:
                query += " limit %s offset %s" % (limitNum - row, row if not offset else row + offsetNum)
            else:
                query += " limit %s offset %s" % (rowPerPage, row if not offset else row + offsetNum)
        elif offset:
            query = re.sub('(\s+offset\s+\d+)', '', query, re.IGNORECASE)
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
    databaseType = request.POST.get('databaseType')     # mysql/redshift/mongoDB
    currentDatabase = request.POST.get('currentDatabase')   # used database/schema
    query = str(request.POST.get('query'))    # input sql query
    query = [sql.replace('\n', ' ').strip() for sql in query.split(';') if sql]
    print('Received Query: ', query)

    # send data
    tableQuery = query          # query that generates each table
    resultAttribute = []        # table attribute
    queryResult = []            # table data
    totalRecords = []           # numeric value of number of rows influenced by a sql query
    influencedRowResult = []    # string value of number of rows influenced by a sql query
    totalTime = 0               # total time of executing all sql queries
    # currentDatabase           # return the current database/schema back in case it changed

    cursor = None
    try:
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
        totalTime += endTime - startTime
        influencedRow = cursor.rowcount
        totalRecords.append(influencedRow)
        # DML (INSERT, UPDATE) will use "affected", DQL (SELECT) will use "retrieved"
        try:
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
            print(influencedRowResult[i])
            print(f'Result Attributes: {resultAttribute[i]}')
            print(f'Query Result: {queryResult[i][:100]}')  # limit query result to 100 rows
            print()
        print(f'Total Execution Time: {round(totalTime, 2)} s')
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
         'totalRecords': totalRecords,
         'influencedRowResult': influencedRowResult,
         'resultAttribute': resultAttribute,
         'queryResult': queryResult,
         'executionTime': str(round(totalTime, 2)) + ' s',
         'currentDatabase': str(currentDatabase)})
