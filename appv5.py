import streamlit as st
import psycopg2
from psycopg2.extras import DictCursor
import xlsxwriter
import pandas as pd
from datetime import datetime, timedelta, date
from collections import defaultdict
from io import BytesIO
import smtplib
from email.message import EmailMessage


# 1. Database Credentials

DB_HOST = "postrgres-hn-dev-uae.postgres.database.azure.com"
DB_PORT = 5432
DB_NAME = "new_fms_fleet_replica"
DB_USER = "hnpgadmin"
DB_PASSWORD = "43ndpzv6TbSHXcZ"


# 2. Fetch Data from Database

def fetch_journey_data(start_date, end_date):

    query = """
    SELECT 
    registration,
    created_at,
    ignition_start_time,
    ignition_end_time,
    driving_duration,
    stop_duration,
    distance_travelled,
    odo_start_reading,
    odo_end_reading,
    start_location,
    end_location,
    name 
    FROM analytics_journey_report
    
    WHERE created_at::DATE BETWEEN %s AND %s
    AND ignition_end_time IS NOT NULL
    ORDER BY created_at DESC;

    """
    conn = None
    data = []
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        with conn.cursor(cursor_factory=DictCursor) as cur:
            start_t = datetime.now()
            cur.execute(query, (start_date, end_date))
            rows = cur.fetchall()

            end_t = datetime.now() - start_t
            print("QUERY TIME:", end_t.total_seconds())

            if not rows:  
                return None 
            for row in rows:
                data.append(dict(row))
            print("Total number count",len(data))
    except Exception as e:
        st.error(f"Error fetching data: {e}")
    finally:
        if conn:
            conn.close()
    return data



# Generating excel Report - Monthwise
def generate_excel_report(data, start_date, end_date):

    output = BytesIO()
    workbook = xlsxwriter.Workbook(output, {'remove_timezone': False})

   
    gray_title_format = workbook.add_format({
        'bold': True,
        'align': 'left',
        'font_size': 11,
        'bg_color': '#D9D9D9'
    })
    header_format = workbook.add_format({
        'bold': True,
        'align': 'center',
        'valign': 'vcenter',
        'bg_color': '#DCE6F1',
        'border': 1
    })
    text_format = workbook.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'border': 1
    })
    number_format = workbook.add_format({
        'num_format': '0.00',
        'border': 1
    })
    subtotal_format = workbook.add_format({
        'bold': True,
        'align': 'right',
        'border': 1
    })
    total_format = workbook.add_format({
        'num_format': '0.00',
        'bold': True,
        'align': 'right',
        'border': 1,
        'bg_color': '#FF0000',
        'font_color': '#FFFFFF'
    })

    
    columns = [
        "Start Time",       
        "Start ODO",        
        "Start Location",
        "End Time",         
        "End ODO",       
        "End Location",
        "Business",         
        "Driving",         
        "Stopped",      
        "Driver"      
    ]

    df = pd.DataFrame(data)


    df['date'] = pd.to_datetime(df['ignition_start_time'].fillna(df['created_at'])).dt.date





    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]




    df['month_year'] = df['date'].apply(lambda d: d.strftime("%B %Y"))

    # Group the data by month_year
    month_groups = df.groupby('month_year')
    sorted_months = sorted(month_groups.groups.keys(), key=lambda m: datetime.strptime(m, "%B %Y"))

    # for each month - excel sheet
    for month_name in sorted_months:

        worksheet = workbook.add_worksheet(month_name)
        current_row = 0


        month_df = month_groups.get_group(month_name)

        reg_groups = month_df.groupby('registration')


        month_total_business = 0.0
        month_total_driving = 0.0
        month_total_stopped = 0.0

        for registration, reg_df in reg_groups:

            worksheet.merge_range(current_row, 0, current_row, len(columns)-1, 
                                  f"Tracker Name: {registration}", gray_title_format)
            current_row += 1
            worksheet.merge_range(current_row, 0, current_row, len(columns)-1, "", gray_title_format)
            current_row += 2

            # Tracker-level totals
            tracker_total_business = 0.0
            tracker_total_driving = 0.0
            tracker_total_stopped = 0.0

            # Group by day within this registration
            day_groups = reg_df.groupby('date')
            sorted_days = sorted(day_groups.groups.keys())
            for day in sorted_days:

                period_str = f"Period: {day.strftime('%d %B %Y')} 00:00 -> {day.strftime('%d %B %Y')} 23:59"
                worksheet.merge_range(current_row, 0, current_row, len(columns)-1, period_str, gray_title_format)
                current_row += 2

                # Write column headers in one row 
                worksheet.write_row(current_row, 0, columns, header_format)
                current_row += 1

                # Initialize day-level totals
                day_total_business = 0.0
                day_total_driving = 0.0
                day_total_stopped = 0.0

                day_df = day_groups.get_group(day).sort_values(by='ignition_start_time', ascending=True)

                for idx, row in day_df.iterrows():

                    start_time_str = row['ignition_start_time'].strftime("%H:%M:%S") if pd.notnull(row['ignition_start_time']) else ""
                    end_time_str = row['ignition_end_time'].strftime("%H:%M:%S") if pd.notnull(row['ignition_end_time']) else ""

                    odo_start = row['odo_start_reading']
                    odo_end = row['odo_end_reading']
                    loc_start = row['start_location']
                    loc_end = row['end_location']
                    distance = row['distance_travelled']
                    driving = row['driving_duration']
                    stopped = row['stop_duration']
                    driver = row['name']
                    

                    row_values = [
                        start_time_str,
                        str(odo_start),
                        loc_start,
                        end_time_str,
                        str(odo_end),
                        loc_end,
                        str(distance),
                        str(driving),
                        str(stopped),
                        driver
                    ]
                    worksheet.write_row(current_row, 0, row_values, text_format)
                    current_row += 1

                   
                    day_total_business += distance
                    day_total_driving += driving
                    day_total_stopped += stopped

                worksheet.write(current_row, 0, "Sub Total", subtotal_format)
                for c in range(1, 6):
                    worksheet.write_blank(current_row, c, None, subtotal_format)
                worksheet.write(current_row, 6, day_total_business, number_format)
                worksheet.write(current_row, 7, day_total_driving, number_format)
                worksheet.write(current_row, 8, day_total_stopped, number_format)
                worksheet.write_blank(current_row, 9, None, subtotal_format)
                current_row += 2

                worksheet.write(current_row, 0, "Total", gray_title_format)
                for c in range(1, 6):
                    worksheet.write_blank(current_row, c, None, gray_title_format)
                worksheet.write(current_row, 6, "Business", header_format)
                worksheet.write(current_row, 7, "Driving", header_format)
                worksheet.write(current_row, 8, "Stopped", header_format)
                worksheet.write(current_row, 9, "Total distance", header_format)
                current_row += 1

                num_entries = len(day_df)
                worksheet.write(current_row, 0, num_entries, total_format)

                for col in range(1, 6):
                    worksheet.write_blank(current_row, col, None, total_format)

                worksheet.write(current_row, 6, day_total_business, total_format)
                worksheet.write(current_row, 7, day_total_driving, total_format)
                worksheet.write(current_row, 8, day_total_stopped, total_format)
                worksheet.write(current_row, 9, day_total_business, total_format)
                current_row += 2

                # Accumulate into tracker-level totals
                tracker_total_business += day_total_business
                tracker_total_driving += day_total_driving
                tracker_total_stopped += day_total_stopped

            # Write Tracker Total row for this registration
            worksheet.write(current_row, 0, "Total", total_format)
            for c in range(1, 6):
                worksheet.write_blank(current_row, c, None, total_format)
            worksheet.write(current_row, 6, tracker_total_business, number_format)
            worksheet.write(current_row, 7, tracker_total_driving, number_format)
            worksheet.write(current_row, 8, tracker_total_stopped, number_format)
            worksheet.write_blank(current_row, 9, None, total_format)
            current_row += 2

            # Accumulate registration totals into month-level totals
            month_total_business += tracker_total_business
            month_total_driving += tracker_total_driving
            month_total_stopped += tracker_total_stopped

        worksheet.write(current_row, 0, "Month Total", total_format)
        for c in range(1, 6):
            worksheet.write_blank(current_row, c, None, total_format)
        worksheet.write(current_row, 6, month_total_business, total_format)
        worksheet.write(current_row, 7, month_total_driving, total_format)
        worksheet.write(current_row, 8, month_total_stopped, total_format)
        worksheet.write_blank(current_row, 9, None, total_format)
        current_row += 2

        # Adjust column widths for the sheet
        worksheet.set_column(0, 0, 12)
        worksheet.set_column(1, 1, 10)
        worksheet.set_column(2, 2, 35)
        worksheet.set_column(3, 3, 12)
        worksheet.set_column(4, 4, 10)
        worksheet.set_column(5, 5, 35)
        worksheet.set_column(6, 8, 10)
        worksheet.set_column(9, 9, 20)

    workbook.close()
    output.seek(0)
    return output


def send_email_with_attachment(sender_email, sender_password, recipient_email, file_bytes, filename="journey_report.xlsx"):
    msg = EmailMessage()
    msg["Subject"] = "Journey Report"
    msg["From"] = sender_email
    msg["To"] = recipient_email
    msg.set_content("Please find attached the journey report.")
    msg.add_attachment(
        file_bytes,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename
    )
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)


def main():
    st.title("Journey Report Generator")

    start_date = st.date_input("Start Date", min_value=date(2024, 1, 1))
    end_date = st.date_input("End Date", min_value=start_date)

    st.write(f"Fetching data from **{start_date}** to **{end_date}**")

    if st.button("Generate and Download Excel"):
        st.cache_data.clear()  
        data = fetch_journey_data(start_date, end_date)

        if data is None or len(data) == 0:
            st.warning(f"No data found between **{start_date} and {end_date}**. Please select another date range.")
            return
        
        start_t1 = datetime.now()

        excel_buffer = generate_excel_report(data, start_date, end_date)

        end_t1 = datetime.now() - start_t1
        print("EXPORT TIME:", end_t1.total_seconds())
        st.download_button(
            label="Download Excel File",
            data=excel_buffer.getvalue(),
            file_name=f"journey_report_{start_date}_{end_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

if __name__ == "__main__":
    main()
