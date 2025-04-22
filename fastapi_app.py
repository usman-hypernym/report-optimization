from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
import psycopg2
from psycopg2.extras import DictCursor
import xlsxwriter
import pandas as pd
from datetime import datetime, date, timedelta
from io import BytesIO
import smtplib
from email.message import EmailMessage
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Journey Report API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database Credentials
DB_HOST = "postrgres-hn-dev-uae.postgres.database.azure.com"
DB_PORT = 5432
DB_NAME = "new_fms_fleet_replica"
DB_USER = "hnpgadmin"
DB_PASSWORD = "43ndpzv6TbSHXcZ"

# Pydantic models for request/response validation
class EmailRequest(BaseModel):
    sender_email: str
    sender_password: str
    recipient_email: str

# Database connection dependency
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

# Function to get date range for last 6 months
def get_last_six_months_range():
    end_date = date.today()
    start_date = end_date - timedelta(days=180)  # Approximately 6 months
    return start_date, end_date

# Fetch journey data function
def fetch_journey_data(start_date: date, end_date: date, conn):
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
    
    data = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query, (start_date, end_date))
            rows = cur.fetchall()
            
            if not rows:
                return None
            for row in rows:
                data.append(dict(row))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
    
    return data

# Excel report generation function
def generate_excel_report(data, start_date: date, end_date: date):
    output = BytesIO()
    workbook = xlsxwriter.Workbook(output, {'remove_timezone': False})
    
    # Format definitions
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
    
    month_groups = df.groupby('month_year')
    sorted_months = sorted(month_groups.groups.keys(), key=lambda m: datetime.strptime(m, "%B %Y"))
    
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
            
            tracker_total_business = 0.0
            tracker_total_driving = 0.0
            tracker_total_stopped = 0.0
            
            day_groups = reg_df.groupby('date')
            sorted_days = sorted(day_groups.groups.keys())
            
            for day in sorted_days:
                period_str = f"Period: {day.strftime('%d %B %Y')} 00:00 -> {day.strftime('%d %B %Y')} 23:59"
                worksheet.merge_range(current_row, 0, current_row, len(columns)-1, period_str, gray_title_format)
                current_row += 2
                
                worksheet.write_row(current_row, 0, columns, header_format)
                current_row += 1
                
                day_total_business = 0.0
                day_total_driving = 0.0
                day_total_stopped = 0.0
                
                day_df = day_groups.get_group(day).sort_values(by='ignition_start_time', ascending=True)
                
                for idx, row in day_df.iterrows():
                    start_time_str = row['ignition_start_time'].strftime("%H:%M:%S") if pd.notnull(row['ignition_start_time']) else ""
                    end_time_str = row['ignition_end_time'].strftime("%H:%M:%S") if pd.notnull(row['ignition_end_time']) else ""
                    
                    row_values = [
                        start_time_str,
                        str(row['odo_start_reading']),
                        row['start_location'],
                        end_time_str,
                        str(row['odo_end_reading']),
                        row['end_location'],
                        str(row['distance_travelled']),
                        str(row['driving_duration']),
                        str(row['stop_duration']),
                        row['name']
                    ]
                    worksheet.write_row(current_row, 0, row_values, text_format)
                    current_row += 1
                    
                    day_total_business += row['distance_travelled']
                    day_total_driving += row['driving_duration']
                    day_total_stopped += row['stop_duration']
                
                worksheet.write(current_row, 0, "Sub Total", subtotal_format)
                for c in range(1, 6):
                    worksheet.write_blank(current_row, c, None, subtotal_format)
                worksheet.write(current_row, 6, day_total_business, number_format)
                worksheet.write(current_row, 7, day_total_driving, number_format)
                worksheet.write(current_row, 8, day_total_stopped, number_format)
                worksheet.write_blank(current_row, 9, None, subtotal_format)
                current_row += 2
                
                tracker_total_business += day_total_business
                tracker_total_driving += day_total_driving
                tracker_total_stopped += day_total_stopped
            
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
        
        # Adjust column widths
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

# Email sending function
def send_email_with_attachment(sender_email: str, sender_password: str, recipient_email: str, file_bytes: BytesIO, filename: str = "journey_report.xlsx"):
    msg = EmailMessage()
    msg["Subject"] = "Journey Report"
    msg["From"] = sender_email
    msg["To"] = recipient_email
    msg.set_content("Please find attached the journey report.")
    msg.add_attachment(
        file_bytes.getvalue(),
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename
    )
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error sending email: {str(e)}")

# API Endpoints
@app.get("/")
async def root():
    return {"message": "Welcome to Journey Report API"}

@app.get("/download-report")
async def download_report(conn = Depends(get_db_connection)):
    start_date, end_date = get_last_six_months_range()
    data = fetch_journey_data(start_date, end_date, conn)
    if not data:
        raise HTTPException(status_code=404, detail="No data found for the specified date range")
    
    excel_buffer = generate_excel_report(data, start_date, end_date)
    
    return StreamingResponse(
        excel_buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename=journey_report_{start_date}_{end_date}.xlsx"
        }
    )

@app.post("/send-report")
async def send_report(email_request: EmailRequest, conn = Depends(get_db_connection)):
    start_date, end_date = get_last_six_months_range()
    data = fetch_journey_data(start_date, end_date, conn)
    if not data:
        raise HTTPException(status_code=404, detail="No data found for the specified date range")
    
    excel_buffer = generate_excel_report(data, start_date, end_date)
    
    send_email_with_attachment(
        email_request.sender_email,
        email_request.sender_password,
        email_request.recipient_email,
        excel_buffer,
        f"journey_report_{start_date}_{end_date}.xlsx"
    )
    
    return {"message": "Report sent successfully"} 