from utils import *


def jc_logs_query(start_time , end_time):
    jc_logs_query_sql = (""" SELECT jccl.rider_cash_id , jccl.actual_receivables ,rc.created_at,r.nic , CONCAT(au.first_name, ' ' , au.last_name) RiderName, c.name,jccl.auth_id ,rc.amount FROM jazz_cash_collection_log jccl INNER JOIN rider_cash rc ON
                            (jccl.rider_cash_id = rc.id) INNER JOIN rider r ON 
                            (rc.rider_id = r.id) INNER join auth_user au on r.user_id=au.id INNER join city c on r.city_id=c.id  WHERE (rc.created_at BETWEEN "2021-05-10 00:00:00" 
                            AND "2021-10-10 00:00:00" AND r.city_id IS NOT NULL) ORDER BY rc.created_at DESC """
                         .format(start_time, end_time))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(jc_logs_query_sql)
    jc_logs = cursor.fetchall()

    return jc_logs


def rc_fuel_log_query(rider_cash_id):
    rc_fuel_log_query_sql = ("""SELECT rfeal.order_ids FROM rider_fuel_earning_adjusted_log rfeal WHERE 
                            rfeal.rider_cash_id = '{}'
                            limit 1 """ .format(rider_cash_id))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(rc_fuel_log_query_sql)
    rc_fuel = cursor.fetchall()
    return rc_fuel[0]


def get_pickup_del_distance(order_ids, start_date, end_date):

     rider_earnings = rider_earnings_query(order_ids, start_date, end_date)

     pickup_distance = pick_distance_query(order_ids, start_date, end_date)
     delivered_distance = delivered_distance_query(order_ids, start_date, end_date)
     return float(pickup_distance), float(delivered_distance)



def rider_earnings_query(order_ids, start_date, end_date):

    rider_earnings_query_sql = ("""SELECT * FROM rider_earnings re
                                WHERE (re.created_at BETWEEN '{}'
                                AND '{}' AND re.order_id IN ('{}'))
                                """.format(start_date, end_date, order_ids))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(rider_earnings_query_sql)
    rider_earnings = cursor.fetchall()
    return rider_earnings


def pick_distance_query(order_ids, start_date, end_date):
    pick_distance_query_sql = ("""SELECT SUM(od.pickup_distance) as pickup_dist FROM rider_earnings re  LEFT join `order` o on re.order_id = o.id
                            INNER JOIN order_distance od on o.id = od.order_id
                            WHERE (re.created_at BETWEEN '{}' AND
                            '{}' AND re.order_id IN ('{}') AND
                            re.log_type = "PB")""".format(start_date, end_date, order_ids))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(pick_distance_query_sql)
    pick_distance = cursor.fetchall()
    return pick_distance[0][0] or 0


def delivered_distance_query(order_ids, start_date, end_date):
    delivered_distance_query_sql = ("""SELECT SUM(od.delivered_distance) as delivered_dist FROM rider_earnings re   LEFT join `order` o on re.order_id = o.id 
                                    INNER JOIN order_distance od on o.id = od.order_id 
                                    WHERE 
                                    (re.created_at BETWEEN '{}' AND '{}' 
                                    AND re.order_id IN ('{}') AND re.log_type = "DDP")"""
                                    .format(start_date, end_date, order_ids))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(delivered_distance_query_sql)
    pick_distance = cursor.fetchall()
    return pick_distance[0][0] or 0

def earnings_data_query(order_ids, start_time, end_time):
    earnings_data_query_sql = (""" SELECT SUM(CASE when re.log_type="PB" then amount  END) as pick_up_distance_bonus ,
                                 SUM(CASE when re.log_type="DDP" then amount  END) as drop_off_distance_pay
                                FROM rider_earnings re   WHERE (re.created_at BETWEEN '{}' AND '{}' AND 
                                re.order_id IN ('{}'))""".format(start_time, end_time, order_ids))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(earnings_data_query_sql)
    earning_query = cursor.fetchall()
    return {
        'pick_up_distance_bonus' : earning_query[0][0],
        'drop_off_distance_pay' : earning_query[0][1]
    }