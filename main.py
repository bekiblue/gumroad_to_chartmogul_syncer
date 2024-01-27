import  requests
import time
from helper import create_database, add_sale, datasource_exists,customer_exists,sale_exists,plan_exists,add_datasource,add_plan, add_customer,close_database
from datetime import datetime,timezone
from dateutil.relativedelta import relativedelta


# Connect to SQLite database
conn, cursor = create_database()




# Your Gumroad access tokens
access_tokens = ["ACCESS_TOKEN_1","ACCESS_TOKEN2","ACCESS_TOKEN_3","ACCESS_TOKEN_4","ACCESS_TOKEN_5"]
current_access_token_index=0


# Replace 'YOUR_API_KEY' with your actual ChartMogul API key
api_key = 'YOUR_API_KEY'



def switch_access_token():
    global current_access_token_index
    # Increment the index to switch to the next access token
    current_access_token_index = (current_access_token_index + 1) % len(access_tokens)
    # Print a message indicating the switch
    print(f"access token switched ")

def fetch_products(next_page_key=None, max_retries=5, retry_delay=5):
    base_url = "https://api.gumroad.com/v2/products"
    headers = {}
    access_token = access_tokens[current_access_token_index]
    params = {'access_token': access_token}
    if next_page_key:
        params["page_key"] = next_page_key
    retries = 0
    while retries < max_retries:
        response = requests.get(base_url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 400:
            print("A required parameter is missing")
            return None
        elif response.status_code == 401:
            print("An invalid gumroad access token.")
            return None
        else:
            print(f"Rate limit exceeded. Retrying in {retry_delay} seconds (Attempt {retries}/{max_retries})")
            switch_access_token()
            retries += 1
            time.sleep(retry_delay)
    print(f"Maximum retries reached. Unable to fetch product data.")
    return None

def fetch_sales(product_id=None, next_page_key=None, max_retries=5, retry_delay=5):
    base_url = "https://api.gumroad.com/v2/sales"
    headers = {}
    access_token = access_tokens[current_access_token_index]
    params = {'access_token': access_token}
    if next_page_key:
        params["page_key"] = next_page_key

    if product_id:
        params["product_id"] = product_id

    retries = 0
    while retries < max_retries:
        response = requests.get(base_url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 400:
            print("A required parameter is missing")
            return None
        elif response.status_code == 401:
            print("An invalid gumroad access token.")
            return None
        else:
            print(f"Rate limit exceeded. Retrying in {retry_delay} seconds (Attempt {retries}/{max_retries})")
            switch_access_token()
            retries += 1
            time.sleep(retry_delay)
    print(f"Maximum retries reached. Unable to fetch sales data.")
    return None


# Function to create a new datasource
def create_datasource(cursor, datasource_name):
    # The endpoint for creating a datasource
    url = "https://api.chartmogul.com/v1/data_sources"
    headers = {
        "Content-Type": "application/json",
    }
    data = {
        "name": f"{datasource_name}"
    }
    response = requests.post(url, auth=(api_key, ''), json=data, headers=headers)
    # Check if the request was successful (status code 201)
    if response.status_code == 201 or response.status_code == 200 or response.status_code == 202:
        # Extract the uuid from the API response
        created_datasource_uuid = response.json().get("uuid")

        # Store the uuid in your database
        add_datasource(cursor, datasource_name, created_datasource_uuid)

        print(f"A New Datasource '{datasource_name}' created with UUID: {created_datasource_uuid}")
    else:
        # Print an error message if the request was not successful
        print(f"Error creating datasource '{datasource_name}' - HTTP Status Code: {response.status_code}")
        exit()
def isFreeTrial(s):
    #checking if a free trial is applied
    if s['free_trial_ends_on']:
        free_trial_ends_on = datetime.strptime(s['free_trial_ends_on'], '%b %d, %Y')  
        date = datetime.fromisoformat(s['created_at'].replace("Z",""))
        if free_trial_ends_on > date:
            return free_trial_ends_on
    return False

def serviceEnd(s):
    freeTrial=isFreeTrial(s)
    if freeTrial:
        return freeTrial.isoformat()
    d = datetime.strptime(s['created_at'], "%Y-%m-%dT%H:%M:%SZ")
    if s['subscription_duration'] == "monthly":
        d = d + relativedelta(months=+1) + relativedelta(days=-1)
    elif s['subscription_duration'] == "quarterly":
        d = d + relativedelta(months=+3)+ relativedelta(days=-1)
    elif s['subscription_duration'] == "biannually":
        d = d + relativedelta(months=+6) + relativedelta(days=-1)
    else:
        d = d + relativedelta(years=+1)
    return d.isoformat() 

def getCurr(symbol):
    #handle for indian currency
    if symbol == "$":
        return "USD"
    elif symbol == "€":
        return "EUR"
    elif symbol == "£":
        return "GBP"
    return "USD"
     
product_page=1
products_response = fetch_products()
while products_response and products_response.get("success"):
    for product in products_response["products"]:
        datasource_name=product["name"]
        # Check if datasource with the given name exists
        if not datasource_exists(cursor,datasource_name):
            # If it doesn't exist, create a new datasource
            create_datasource(cursor,datasource_name)
        # Your datasource ID
        dsID=datasource_exists(cursor,datasource_name)

        product_id=product["id"]

        page=1
        # Creating a dictionary for bulk import
        bulk_data = {
            "external_id": datetime.now(timezone.utc).isoformat(),
            "customers": [],
            "plans": [],
            "invoices": [],
            "line_items": [],
            "transactions": [],
            "subscription_events": []
        }
        # Fetch the first page of sales
        sales_response = fetch_sales(product_id)
        # print(sales_response)
        while sales_response and sales_response.get("success"):
            print(f'*****Executing page {page} sales***** for the the product named{product["name"]}')
            # Iterate through sales and populate the dictionary
            if "sales" in sales_response:
                for sale in sales_response["sales"]:
                    # Handling if the sale already exists on the database and the sale condition is changed
                    sale_id = sale["id"]
                    
                    if sale_exists(cursor, sale_id):
                        #skipping if it was processed previously
                        print("This sale has been previously processed.")
                        continue
                    # Mark the sale as processed in the database
                    add_sale(cursor, sale_id)
        

                    print(f"Processing a recent sale for a product named {sale['product_name']} initiated at {sale['created_at']}")
                    price = sale['price']
                    type= "one_time"
                    #prepare the Customer data if a new customer is encountered
                    if not customer_exists(cursor, sale["email"], dsID):
                        customer_data = {
                            "data_source_uuid" : dsID,
                            "external_id": sale["email"],
                            "name":sale['full_name'] if 'full_name' in sale else sale['email'].split('@')[0],
                            "email": sale["email"],
                            }
                        if sale["is_recurring_billing"] and isFreeTrial(sale):
                            customer_data['free_trial_started_at']=sale["created_at"]
                        if "country_iso2" in sale:
                                customer_data["country"]=sale["country_iso2"] 
                        #add it to the dictionary   
                        add_customer(cursor,sale["email"],dsID)
                        bulk_data["customers"].append(customer_data)


                    # Plan data for subscription products
                    if sale["is_recurring_billing"]:
                        type="subscription"            
                        if isFreeTrial(sale):
                            price = 0 
                        plan_name=sale['variants']['Tier'] + " " + sale['subscription_duration']
                        if not plan_exists(cursor, plan_name,dsID):
                            # If the plan doesn't exist in the database, add it
                            add_plan(cursor, plan_name,dsID)
                            # If the plan doesn't exists on the database create new
                            if  sale['subscription_duration'] == "monthly":
                                interval_unit="month"
                                interval_count=1
                            elif sale['subscription_duration'] == "quarterly":
                                interval_unit="month"
                                interval_count=3
                            elif sale['subscription_duration'] == "biannually":
                                interval_unit="month"
                                interval_count=6
                            else:
                                interval_unit="year"
                                interval_count=1
                            plan_data = {
                            "name": plan_name,
                            "external_id": plan_name,
                            "interval_count": interval_count,
                            "interval_unit": interval_unit}
                            #add it to the dictionary   
                            bulk_data["plans"].append(plan_data)


                    #invoice data
                    invoice_data = {
                    "external_id": sale["id"],
                    "date": sale["created_at"],
                    "currency": getCurr(sale['currency_symbol']),  
                    "customer_external_id": sale["email"], }
                    #add it to the dictionary   
                    bulk_data["invoices"].append(invoice_data )

                    
                    
                    #line item data
                    line_item_data = {
                                        "type": type,  
                                        "amount_in_cents": price,
                                        "quantity": sale["quantity"], 
                                        "invoice_external_id": sale["id"],
                                    }
                    if sale["is_recurring_billing"] :
                        line_item_data["plan_external_id"]=plan_name 
                        line_item_data["subscription_external_id"]=sale["subscription_id"]
                        line_item_data["service_period_start"]=sale["created_at"]
                        line_item_data["service_period_end"]=serviceEnd(sale)
                    #add it to the dictionary   
                    bulk_data["line_items"].append(line_item_data)



                    #transaction data
                    transaction_data = {
                    "invoice_external_id":sale["id"],
                    "type": "payment",
                    "result": "successful",
                    "date": sale["created_at"]
                    }
                    bulk_data["transactions"].append(transaction_data)
                    


            
            if page == 20:
                    print("syncing")
                    #sync the updates
                    url = f"https://api.chartmogul.com/v1/data_sources/{dsID}/json_imports"
                    headers = {
                        "Content-Type": "application/json",
                    }
                    chart_response=requests.post(url, auth=(api_key, ''), json=bulk_data, headers=headers)
                    # Print the response from the API
                    if chart_response.status_code == 201 or chart_response.status_code == 200 or chart_response.status_code == 202 :
                        print(f"All recently processed sales have been successfully synchronized for product{datasource_name}")
                        page=1
                        bulk_data = {
                                        "external_id": datetime.now(timezone.utc).isoformat(),
                                        "customers": [],
                                        "plans": [],
                                        "invoices": [],
                                        "line_items": [],
                                        "transactions": [],
                                        "subscription_events": []
                                    }
                    elif chart_response.status_code == 400:
                        print("A required parameter is missing")
                        exit()
                    elif chart_response.status_code == 401:
                        print("An invalid chart mogul API Key")
                        exit()
                    else:
                        print(f"An error have occured on chartmogul with error code {chart_response.status_code}")
                        exit()

        
            time.sleep(1)
            # Check if there is a next page of sales for the product
            next_page_key = sales_response.get("next_page_key")
            if next_page_key:
                print("There is another page of sales to process for this product")
                page+=1
                # Fetch the next page of sales using the next_page_key
                sales_response = fetch_sales(product_id,next_page_key)

            else:
                print("syncing the last pages of sales for the product")
                #sync the last updates
                url = f"https://api.chartmogul.com/v1/data_sources/{dsID}/json_imports"
                headers = {
                    "Content-Type": "application/json",
                }
                chart_response=requests.post(url, auth=(api_key, ''), json=bulk_data, headers=headers)

                # Print the response from the API
                if chart_response.status_code == 201:
                    print("All recently processed sales have been successfully synchronized.")
                elif chart_response.status_code == 400:
                    print("A required parameter is missing")
                elif chart_response.status_code == 401:
                    print("An invalid chart mogul API Key")
                else:
                    print(f"An error have occured on chartmogul with error code {chart_response.status_code}")
                        # No more pages, exit the loop
                break
    time.sleep(1)
    # Check if there is a next page of products
    next_page_key = products_response.get("next_page_key")
    if next_page_key:
                print("There is another page of products to process")
                product_page+=1
                # Fetch the next page of sales using the next_page_key
                products_response = fetch_products(next_page_key)
    else:
        # No more pages, exit the loop
        break




# Close the database connection
close_database(conn)