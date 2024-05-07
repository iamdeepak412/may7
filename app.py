
import os
from dotenv import load_dotenv
from flask import Flask, jsonify, request
import mysql.connector
from datetime import datetime, timedelta
import json
from flasgger import Swagger

app = Flask(__name__)
swagger = Swagger(app)

load_dotenv()

# Retrieve database credentials from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# MySQL database connection details
db_connection = mysql.connector.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)



# Function to execute SQL query with proper error handling
def execute_query(query, params=None):
    cursor = db_connection.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results[0][0] if results else None
    except Exception as e:
        print("Error executing query:", e)
        cursor.close()
        return None

# Route to fetch personal details
@app.route('/personal')
def personal():
    """
    Fetch personal details for a user.
    ---
    parameters:
      - name: u_id
        in: query
        type: string
        required: true
        description: The unique identifier of the user.
    responses:
      200:
        description: Personal details fetched successfully.
    """
    u_id = request.args.get('u_id')

    if not u_id:
        return jsonify({"error": "uId parameter is missing."}), 400

    # Queries to fetch data for the specified u_id
    query_personalDetailsCount = f"SELECT personal_details_count FROM user_profile WHERE u_id = '{u_id}';"
    query_gender = f"SELECT gender FROM user_profile WHERE u_id = '{u_id}';"
    query_currentCity = f"SELECT current_city FROM user_profile WHERE u_id = '{u_id}';"
    query_currentState = f"SELECT current_state FROM user_profile WHERE u_id = '{u_id}';"
    query_zipcode = f"SELECT zipcode FROM user_profile WHERE u_id = '{u_id}';"
    query_currentAddress = f"SELECT current_address FROM user_profile WHERE u_id = '{u_id}';"
    query_maritalStatus = f"SELECT marital_status FROM user_profile WHERE u_id = '{u_id}';"
    query_lastWorkingDay = f"SELECT lwd FROM user_profile WHERE u_id = '{u_id}';"
    query_dateOfBirth = f"SELECT dob FROM user_profile WHERE u_id = '{u_id}';"
    query_otherDetails = f"SELECT other_details FROM user_profile WHERE u_id = '{u_id}';"
    query_coverLetter = f"SELECT cover_letter FROM user_profile WHERE u_id = '{u_id}';"
    query_resumeLink = f"SELECT resume_link FROM user_profile WHERE u_id = '{u_id}';"
    query_govtIdLink = f"SELECT govt_id_link FROM user_profile WHERE u_id = '{u_id}';"
    query_profilePicLink = f"SELECT profile_pic_link FROM user_profile WHERE u_id = '{u_id}';"
    query_coverPicLink = f"SELECT cover_pic_link FROM user_profile WHERE u_id = '{u_id}';"
    query_socialLinks = f"SELECT social_links FROM user_profile WHERE u_id = '{u_id}';"
    query_languagesKnown = f"SELECT languages_known FROM user_profile WHERE u_id = '{u_id}';"
    query_profileCompletion = f"SELECT profile_completion FROM user_profile WHERE u_id = '{u_id}';"
    query_profileCreatedby = f"SELECT profile_createdby FROM user_profile WHERE u_id = '{u_id}';"
    query_profileCreatedts = f"SELECT profile_createdts FROM user_profile WHERE u_id = '{u_id}';"
    query_profileUpdatedby = f"SELECT profile_updatedby FROM user_profile WHERE u_id = '{u_id}';"
    query_profileUpdatedts = f"SELECT profile_updatedts FROM user_profile WHERE u_id = '{u_id}';"
    query_servingNoticePeriod = f"SELECT is_serving_np FROM user_profile WHERE u_id = '{u_id}';"
    query_u_id = f"SELECT u_id FROM page_view WHERE u_id = '{u_id}';"

    # Execute queries to get data for the specified u_id
    personalDetailsCount = execute_query(query_personalDetailsCount)
    gender = execute_query(query_gender)
    currentCity = execute_query(query_currentCity)
    currentState = execute_query(query_currentState)
    zipcode = execute_query(query_zipcode)
    currentAddress = execute_query(query_currentAddress)
    maritalStatus = execute_query(query_maritalStatus)
    lastWorkingDay = execute_query(query_lastWorkingDay)
    dateOfBirth = execute_query(query_dateOfBirth)
    otherDetails = execute_query(query_otherDetails)
    coverLetter = execute_query(query_coverLetter)
    resumeLink = execute_query(query_resumeLink)
    govtIdLink = execute_query(query_govtIdLink)
    profilePicLink = execute_query(query_profilePicLink)
    coverPicLink = execute_query(query_coverPicLink)
    socialLinks = execute_query(query_socialLinks)
    languagesKnown = execute_query(query_languagesKnown)
    profileCompletion = execute_query(query_profileCompletion)
    profileCreatedby = execute_query(query_profileCreatedby)
    profileCreatedts = execute_query(query_profileCreatedts)
    profileUpdatedby = execute_query(query_profileUpdatedby)
    profileUpdatedts = execute_query(query_profileUpdatedts)
    servingNoticePeriod = execute_query(query_servingNoticePeriod)
    u_id = execute_query(query_u_id)

    # Response data
    response_data = {
        "coverLetter": coverLetter,
        "coverPicLink": coverPicLink,
        "currentAddress": currentAddress,
        "currentCity": currentCity,
        "currentState": currentState,
        "dateOfBirth": dateOfBirth,
        "gender": gender,
        "govtIdLink": govtIdLink,
        "languagesKnown": languagesKnown,
        "lastWorkingDay": lastWorkingDay,
        "maritalStatus": maritalStatus,
        "otherDetails": otherDetails,
        "personalDetailsCount": personalDetailsCount,
        "profileCompletion": profileCompletion,
        "profileCreatedby": profileCreatedby,
        "profileCreatedts": profileCreatedts,
        "profilePicLink": profilePicLink,
        "profileUpdatedby": profileUpdatedby,
        "profileUpdatedts": profileUpdatedts,
        "resumeLink": resumeLink,
        "servingNoticePeriod": servingNoticePeriod,
        "socialLinks": socialLinks,
        "uid": u_id,
        "zipcode": zipcode
    }

    # Check if print_response parameter is present in the query string
    print_response = request.args.get('print_response', False)
    if print_response and print_response.lower() == 'true':
        print(json.dumps(response_data, indent=4))

    return jsonify(response_data)

# Route to fetch platform statistics
@app.route('/platform')
def platform():
    """
    Fetch platform statistics.
    ---
    responses:
      200:
        description: Platform statistics fetched successfully.
    """
    # Queries to count hackathons of different natures
    query_active_jobs = "SELECT COUNT(*) FROM jobs WHERE is_active = 1;"
    query_total_users = "SELECT COUNT(*) FROM user;"
    query_job_views = "SELECT COUNT(*) FROM job_view;"
    query_total_jobs = "SELECT COUNT(*) FROM jobs;"
    query_page_view = "SELECT COUNT(*) FROM page_view;"

    # Execute queries to get counts
    active_jobs = execute_query(query_active_jobs)
    total_users = execute_query(query_total_users)
    job_views = execute_query(query_job_views)
    total_jobs = execute_query(query_total_jobs)
    page_view = execute_query(query_page_view)

    response_data = {
        "numberOfActiveJobs": active_jobs,
        "numberOfUsers": total_users,
        "numberofJobViews": job_views,
        "numberOfJobs": total_jobs,
        "numberofSiteViews": page_view,
        # "pie_chart": pie_chart.to_json(),
        # "bar_chart": bar_chart.to_json()
    }

    # Check if print_response parameter is present in the query string
    print_response = request.args.get('print_response', False)
    if print_response and print_response.lower() == 'true':
        print(json.dumps(response_data, indent=4))

    return jsonify(response_data)

# Function to fetch data for the specified range of days
def fetch_stats_data(range_to_fetch):
    start_date = datetime.now() - timedelta(days=range_to_fetch)
    query = f"SELECT * FROM platform_stats WHERE record_insertedts >= '{start_date}' ORDER BY record_insertedts DESC;"
    cursor = db_connection.cursor(dictionary=True)
    cursor.execute(query)
    stats_info = cursor.fetchall()
    cursor.close()
    return stats_info

# Mapping between original keys and desired keys
key_mapping = {
    "stats_id": "statsId",
    "numb_active_users": "numberOfActiveUsersOnPlatform",
    "numb_inactive_users": "numberOfInactiveUsersOnPlatform",
    "numb_jobs": "numberOfJobPosted",
    "numb_partners": "numberOfPartners",
    "numb_joinings": "joiningsDone",
    "numb_users_in_one_day": "numberOfUsersInOneDay",
    "numb_active_users_in_one_day": "numberOfActiveUsersInOneDay",
    "numb_inactive_users_in_one_day": "numberOfInactiveUsersInOneDay",
    "numb_jobs_in_one_day": "numberOfJobPostedInOneDay",
    "numb_platform_views_in_one_day": "numberOfPlatformViewsInOneDay",
    "numb_job_views_in_one_day": "numberOfJobViewsInOneDay",
    "numb_drill_views_in_one_day": "numberOfDrillViewsInOneDay",
    "numb_landing_views_in_one_day": "numberOfLandingViewsInOneDay",
    "numb_profile_views_in_one_day": "numberOfProfileViewsInOneDay",
    "numb_drill_participants_in_one_day": "numberOfDrillParticipantsInOneDay",
    "numb_jobs_applied_in_one_day": "numberOfJobsAppliedInOneDay",
    "all_pages_stats": "allPagesStats",
    "user_updatedby": "userUpdatedby",
    "record_insertedts": "recordInsertedts",
    "numb_squad": "numberOfSquadPrograms",
    "numb_hackathons": "numberOfHackathons",
    "numb_professionals_users": "numberOfProfessionalsUsers",
    "numb_freshers_users": "numberOfFreshersUsers",
    "numb_fresher_hired": "numberOfFresherHired",
    "numb_interns_hired": "numberOfInternsHired",
    "numb_meetups": "numberOfmeetups",
    "numb_colleges": "numberOfColleges",
    "numb_fresher_drives": "numberOfFresherDrives",
    "numb_students_in_squad": "numberOfStudentsInSquad"
}

# Function to format response
def format_response(stats_data):
    formatted_data = []
    for entry in stats_data:
        formatted_entry = {}
        for original_key, new_key in key_mapping.items():
            formatted_entry[new_key] = entry.get(original_key)
        formatted_entry["recordInsertedts"] = formatted_entry["recordInsertedts"].isoformat() if formatted_entry["recordInsertedts"] else None
        formatted_data.append(formatted_entry)
    return formatted_data

@app.route('/stats', methods=['GET'])
def get_stats_data():
    """
    Fetch platform statistics for a specified range of days.
    ---
    parameters:
      - name: rangeToFetch
        in: query
        type: integer
        required: true
        description: Number of days for which statistics are to be fetched.
    responses:
      200:
        description: Platform statistics fetched successfully.
    """
    try:
        range_to_fetch = int(request.args.get('rangeToFetch'))
        stats_data = fetch_stats_data(range_to_fetch)
        formatted_data = format_response(stats_data)
        return jsonify(formatted_data)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/customer')
def customer():
    """
    Fetch customer details.
    ---
    parameters:
      - name: partner_id
        in: query
        type: string
        required: true
        description: The unique identifier of the partner.
    responses:
      200:
        description: Customer details fetched successfully.
    """
    partner_id = request.args.get('partner_id')

    if not partner_id:
        return jsonify({"error": "partner_id parameter is missing."}), 400

    # Define SQL queries with placeholders for parameters
    queries = {
        "partnerId": "SELECT partner_id FROM partner WHERE partner_id = %s;",
        "partnerName": "SELECT partner_name FROM partner WHERE partner_id = %s;",
        "partnerDisplayName": "SELECT partner_display_name FROM partner WHERE partner_id = %s;",
        "partnerDescription": "SELECT partner_desc FROM partner WHERE partner_id = %s;",
        "partnerSocialLinks": "SELECT partner_sociallinks FROM partner WHERE partner_id = %s;",
        "partnerType": "SELECT partner_type FROM partner WHERE partner_id = %s;",
        "partnerIndustry": "SELECT partner_industry FROM partner WHERE partner_id = %s;",
        "partnerEstablishmentDate": "SELECT partner_estab_date FROM partner WHERE partner_id = %s;",
        "partnerLogoPath": "SELECT partner_logo_path FROM partner WHERE partner_id = %s;",
        "headquarter": "SELECT headquarter FROM partner WHERE partner_id = %s;",
        "location": "SELECT location FROM partner WHERE partner_id = %s;",
        "gstin": "SELECT gstin FROM partner WHERE partner_id = %s;",
        "pan": "SELECT pan FROM partner WHERE partner_id = %s;",
        "address": "SELECT address FROM partner WHERE partner_id = %s;",
        "subCategory": "SELECT sub_category FROM partner WHERE partner_id = %s;",
        "representativeUid": "SELECT representative_uid FROM partner WHERE partner_id = %s;",
        "mailEmail": "SELECT mail_email FROM partner WHERE partner_id = %s;",
        "mailDisplayName": "SELECT mail_display_name FROM partner WHERE partner_id = %s;",
        "representativeUserDetails": "SELECT representative_user_details FROM partner WHERE partner_id = %s;",
        "createdTs": "SELECT record_createdts FROM partner WHERE partner_id = %s;",
        "updatedTs": "SELECT record_updatedts FROM partner WHERE partner_id = %s;",
        "createdBy": "SELECT record_createdby FROM partner WHERE partner_id = %s;",
        "updatedBy": "SELECT record_updatedby FROM partner WHERE partner_id = %s;",
        "active": "SELECT is_active FROM partner WHERE partner_id = %s;"
    }

    # Execute queries and store results in response_data
    response_data = {}
    for key, query in queries.items():
        response_data[key] = execute_query(query, (partner_id,))

    # Check if print_response parameter is present in the query string
    print_response = request.args.get('print_response', False)
    if print_response and print_response.lower() == 'true':
        print(json.dumps(response_data, indent=4))

    return jsonify(response_data)

if __name__ == '__main__':
    app.run(debug=True, port=5000)

