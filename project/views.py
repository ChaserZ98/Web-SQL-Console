from django.shortcuts import render
# from django.http import HttpResponse
from django.http import JsonResponse
# import mysql.connector as mysql
import time
from django.db import connections


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
    cursor = connections[databaseType].cursor()
    result = ''
    if databaseType == 'mysql':
        cursor.execute('select database()')
        result = cursor.fetchone()[0]
    elif databaseType == 'redshift':
        cursor.execute('select current_database()')
        result = cursor.fetchone()[0]
    return JsonResponse({'currentDatabase': result})


def execute_sql(sql, conn):
    """
    :param sql: sql query
    :param conn: database connection instance
    :return: query result
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


# receive ajax request and return requested data
def ajax(request):
    """
    :param request: http request
    :return: json response including query result
    """
    # received data
    databaseType = request.POST.get('databaseType')     # mysql/redshift/mongoDB
    currentDatabase = request.POST.get('currentDatabase')   # used database/schema
    query = str(request.POST.get('query')).replace('\n', '')    # input sql query
    query = [sql for sql in query.split(';') if sql]
    print('Received Query: ', query)

    # send data
    resultAttribute = []        # table attribute
    queryResult = []            # table data
    influencedRowResult = []    # number of rows influenced by a sql query
    totalTime = 0               # total time of executing all sql queries
    # currentDatabase           # return the current database/schema back in case it changed

    cursor = connections[databaseType].cursor()
    for sql in query:
        startTime = time.time()
        influencedRow = cursor.execute(sql)
        endTime = time.time()
        totalTime += endTime - startTime
        if influencedRow == 0 or influencedRow is None:
            influencedRowResult.append(None)
        elif influencedRow == 1:
            temp = f"{influencedRow} row "
            temp += "retrieved" if cursor.description else "affected"
            influencedRowResult.append(temp)
        else:
            temp = f"{influencedRow} rows "
            temp += "retrieved" if cursor.description else "affected"
            influencedRowResult.append(temp)
        queryResult.append(cursor.fetchall())
        if cursor.description:
            resultAttribute.append([attribute[0] for attribute in cursor.description])
        else:
            resultAttribute.append(None)

    for i in range(len(influencedRowResult)):
        print(f'Executed Query: {query[i]}')
        print(influencedRowResult[i])
        print(f'Result Attributes: {resultAttribute[i]}')
        print(f'Query Result: {queryResult[i]}')
        print()
    print(f'Total Execution Time: {round(totalTime, 2)} s')

    if databaseType == 'mysql':
        cursor.execute('select database()')
        currentDatabase = cursor.fetchone()[0]
    elif databaseType == 'redshift':
        cursor.execute('select current_database()')
        currentDatabase = cursor.fetchone()[0]
    return JsonResponse({'influencedRowResult': influencedRowResult, 'resultAttribute': resultAttribute,  'queryResult': queryResult, 'executionTime': str(round(totalTime, 2)) + ' s',
                         'currentDatabase': str(currentDatabase)})
