import datetime

from utils import *
from sql import *
from datetime import timedelta,timezone

NAME = 'Rider'
NIC = 'NIC'
RIDER_CITY = 'Rider City'
PICKUP_PAY = 'Pickup Pay'
PICKUP_DISTANCE = "Pickup Distance KM"
DROP_OFF_PAY = 'Drop-off Pay'
DROP_OFF_DISTANCE = "Drop-off Distance KM"
FUEL_ALLOWANCE = "Fuel Allowance"
INITIAL_RECEIVABLE = 'Initial Receivable'
RECEIVABLES_AT_DEPOSITED_TIME = "Receivables at Deposited Time"
AMOUNT_RECEIVED = "Amount Received"
RECEIVABLES_LEFT = "Receivables Left"
DEPOSITED_AT = "Deposited At"
JAZZ_CASH_ID = "Jazzcash ID"


def rider_cash_collection(start_date, end_date):
    data = get_dates(start_date, end_date)
    start_time, end_time = data['start_time'], data['end_time']
    print('time' , type(start_time))
    start_date, end_date = data['start_date'], data['end_date']
    riders_data = []
    jc_logs = jc_logs_query(start_time, end_time)


    for jc_log in jc_logs:

        pick_up_distance_bonus = pickup_distance = drop_off_distance_pay = delivered_distance = fuel_allowance = 0
        rc_fuel_log = rc_fuel_log_query(jc_log[0])

        if rc_fuel_log:
            order_ids = rc_fuel_log[0]

            if len(order_ids) > 0:
                pickup_distance, delivered_distance = get_pickup_del_distance(order_ids, start_time, end_time)
                earnings_data = earnings_data_query(order_ids, start_time, end_time)
                pick_up_distance_bonus = earnings_data['pick_up_distance_bonus'] or 0
                drop_off_distance_pay = earnings_data['drop_off_distance_pay'] or 0
                fuel_allowance = pick_up_distance_bonus + drop_off_distance_pay

        receivables_without_fuel_amount = jc_log[1] - fuel_allowance
        updated_deposited_time = str(jc_log[2] + timedelta(hours=5))
        print('update time', updated_deposited_time)

        riders_data.append({NAME: jc_log[4], NIC: jc_log[3],
                            RIDER_CITY: jc_log[5],
                            PICKUP_PAY: pick_up_distance_bonus,
                            PICKUP_DISTANCE: pickup_distance,
                            DROP_OFF_PAY: drop_off_distance_pay,
                            DROP_OFF_DISTANCE: delivered_distance,
                            FUEL_ALLOWANCE: fuel_allowance,
                            INITIAL_RECEIVABLE: jc_log[1],
                            RECEIVABLES_AT_DEPOSITED_TIME: receivables_without_fuel_amount,
                            AMOUNT_RECEIVED: jc_log[7],
                            RECEIVABLES_LEFT: receivables_without_fuel_amount - jc_log[7],
                            DEPOSITED_AT: updated_deposited_time,
                            JAZZ_CASH_ID: jc_log[6]
                            })
    header = [NAME, NIC, RIDER_CITY, PICKUP_PAY, PICKUP_DISTANCE, DROP_OFF_PAY,
              DROP_OFF_DISTANCE, FUEL_ALLOWANCE, INITIAL_RECEIVABLE, RECEIVABLES_AT_DEPOSITED_TIME, AMOUNT_RECEIVED,
              RECEIVABLES_LEFT, DEPOSITED_AT, JAZZ_CASH_ID]

    title = 'Jazzcash Rider Cash Collection Report - ({} - {})'.format(start_time, end_time)
    file_name = '{}.csv'.format(title)
    zip_file = create_csv(file_name, riders_data, header)
    attachments = [{'name': file_name + '.zip', 'content': zip_file.getvalue()}]
    # send_report_as_email(title=title, template=None, attachments=attachments, recipients=[email])
    print('rider',riders_data)
    # import csv
    #
    #
    # with open('countries.csv', 'w', encoding='UTF8') as f:
    #     writer = csv.writer(f)
    #
    #     # write the header
    #     writer.writerow(header)
    #
    #     # write the data
    #     writer.writerow(riders_data)



rider_cash_collection("2021-05-10", "2021-10-10")

