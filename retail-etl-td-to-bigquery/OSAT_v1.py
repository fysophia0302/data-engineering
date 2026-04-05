import os
import time
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    time.sleep(5)
    page.goto("https://cloud.inmoment.com/program/ck8t73m0dilg108945xfozc87/home")
    
    
    page.wait_for_selector("#appLauncherIcon", timeout=0)

    page.click("#appLauncherIcon")
    page.click("div.tile-text-wrapper:has-text('CXIC Reporting')")
    page.click("text=RETAIL IN-STORE - CORE REPORTS")
    page.wait_for_selector("text=Retail Summary Report v2", timeout=60000)
    page.click("text= Retail Summary Report v2")
    
    
    iframe_element = page.query_selector("iframe[src*='/report/app']")
    
    if iframe_element:
        target_frame = iframe_element.content_frame()
        if target_frame:
            print("get the correct frame")
        else:
            print("not load yet")
    else:
        print("iframe missing")


    Super_Market=['Core Market','Fortinos Division','Superstore']
    
    
    def fetch_totals_data():
        try:
            # wait selection-list disappear
            target_frame.wait_for_selector("div#selection-list", state="hidden", timeout=180000)
            print("selection-list disappeared, fetching textAnalyticsGrid table...")

            # wait the table has data
            target_frame.wait_for_function(
                """() => {
                    const table = document.querySelector('table#textAnalyticsGrid');
                    return table && table.querySelectorAll('tbody tr').length > 0;
                }""",
                timeout=30000
            )

            # html
            table_html = target_frame.inner_html("table#textAnalyticsGrid")
            table_html = '<table>' + table_html + '</table>'

            # to DF
            df = pd.read_html(StringIO(table_html))[0]
            df = df[['Unnamed: 2', 'In-Store OSAT', 'Good value for money (Top Box %)', 'Items In Stock (Top Box %)']]
            totals_data = df[df['Unnamed: 2'] == 'Totals'][['In-Store OSAT', 'Good value for money (Top Box %)', 'Items In Stock (Top Box %)']]

            print(totals_data)
            return totals_data

        except Exception as e:
            print("selection-list did not disappear or table not found:", e)
            return None


    def run_brand_report(brand_list, date_range_value):
        date_range_map = {
            51687: "Last Week",
            51700: "Last 4 Weeks",
            51702: "Last 12 Weeks"
        }
        target_frame.wait_for_selector("#quickDateRangeSelect", timeout=60000)
        target_frame.select_option("#quickDateRangeSelect", value=str(date_range_value))

        date_range_text = date_range_map.get(date_range_value, "Unknown Range")
        print(f"Date range {date_range_text} ({date_range_value}) has been selected.")

        target_frame.wait_for_selector("#dummySelector", timeout=60000)
        target_frame.click("#dummySelector")
        target_frame.click("div.ng-binding.ng-scope:has-text('Brand Hierarchy')")
        target_frame.locator("div#dummySelector", has_text="Single-Level").click()
        target_frame.locator("div.ng-binding.ng-scope", has_text="Multi-Level").click()

        target_frame.click("i.tree-branch-head")
        for brand in brand_list:
            target_frame.locator("div.tree-label", has_text=brand).click()
        print(f"Brands {brand_list} have been selected.")

        target_frame.click("button.primary.ng-binding:has-text('Apply')")
        target_frame.click("div#runButton[alt='Run Report']")

        return fetch_totals_data()
    
    
    
   #Last week SuperMarkt
    Super_Market_LastWk_data = run_brand_report(Super_Market, 51687)
    #Last 4 week SuperMarkt 51700
    Super_Market_Last4Wk_data = run_brand_report(Super_Market, 51700)
    # #Last 12 week SuperMarkt 51702
    Super_Market_Last12Wk_data = run_brand_report(Super_Market, 51702)
    # #Last week Hard Discount
    Hard_Discount_LastWk_data = run_brand_report(["Hard Discount"], 51687)
    # #Last 4 week SuperMarkt 51700
    Hard_Discount_Last4Wk_data = run_brand_report(["Hard Discount"], 51700)
    # #Last 12 week SuperMarkt 51702
    Hard_Discount_Last12Wk_data = run_brand_report(["Hard Discount"], 51702)

 
 #=================================get the custom date report====================================
 #=================================hard discount====================================

    target_frame.wait_for_selector("#quickDateRangeSelect", timeout=60000)
    target_frame.select_option("#quickDateRangeSelect", value="-1")
    
    
    target_frame.wait_for_selector("#calendarListRangeBeginTrigger", state="visible", timeout=30000)
    time.sleep(2)
    target_frame.fill("#calendarListRangeBeginTrigger", '01/04/26')
    time.sleep(2)
    # print(f"start at 01/04/26")

    today = datetime.today()
    days_since_saturday = (today.weekday() - 5) % 7
    last_saturday = today - timedelta(days=days_since_saturday)
    last_saturday_str = last_saturday.strftime("%m/%d/%y")
    target_frame.wait_for_selector("#calendarListRangeEndTrigger", state="visible", timeout=30000)
    target_frame.fill("#calendarListRangeEndTrigger", last_saturday_str)
    #print(f"end at {last_saturday_str}")
    target_frame.click("body")

    target_frame.wait_for_selector("#dummySelector", timeout=60000)
    target_frame.click("#dummySelector")
    target_frame.click("div.ng-binding.ng-scope:has-text('Brand Hierarchy')")
    target_frame.locator("div#dummySelector", has_text="Single-Level").click()
    target_frame.locator("div.ng-binding.ng-scope", has_text="Multi-Level").click()
    target_frame.click("i.tree-branch-head")

    #select hard discount
    target_frame.locator("div.tree-label", has_text="Hard Discount").click()
    
    target_frame.click("button.primary.ng-binding:has-text('Apply')")       
    target_frame.click("div#runButton[alt='Run Report']")
    print('the 1st extraction for Hard Discount is running')
    ##🚨since there is a web bug, the click steps should be repeated to get the correct data
    
    # iframe_element = page.query_selector("iframe[src*='/report/app']")
    # if not iframe_element:
    #     raise Exception("iframe missing")

    # target_frame = iframe_element.content_frame()
    # if not target_frame:
    #     raise Exception("iframe not loaded yet")
    
    try:
        # ensure target_frame 
        if not target_frame:
            raise Exception("target_frame is None, iframe not ready")
        # wait selection-list disappear
        target_frame.wait_for_selector("div#selection-list", state="hidden", timeout=180000)
        print("selection-list disappeared, fetching textAnalyticsGrid table...")

        # wait the table has data
        target_frame.wait_for_function(
            """() => {
                const table = document.querySelector('table#textAnalyticsGrid');
                return table && table.querySelectorAll('tbody tr').length > 0;
            }""",
            timeout=30000
        )
    except Exception as e:
        print("selection-list did not disappear or table not found:", e)
    
    
    
    # select the costom date option
    target_frame.wait_for_selector("#quickDateRangeSelect", timeout=60000)
    target_frame.select_option("#quickDateRangeSelect", value="-1")
    # input '01/04/26'
    target_frame.wait_for_selector("#calendarListRangeBeginTrigger", state="visible", timeout=60000)
    time.sleep(2)
    target_frame.fill("#calendarListRangeBeginTrigger", '01/04/26')
    time.sleep(2)
    print(f"start at 01/04/26")


    target_frame.wait_for_selector("#calendarListRangeEndTrigger", state="visible", timeout=60000)
    target_frame.fill("#calendarListRangeEndTrigger", last_saturday_str)
    print(f"end at {last_saturday_str}")
    target_frame.click("body")

    target_frame.wait_for_selector("#dummySelector", timeout=60000)
    target_frame.click("#dummySelector")
    target_frame.click("div.ng-binding.ng-scope:has-text('Brand Hierarchy')")
    target_frame.locator("div#dummySelector", has_text="Single-Level").click()
    target_frame.locator("div.ng-binding.ng-scope", has_text="Multi-Level").click()
    target_frame.click("i.tree-branch-head")

    
    target_frame.locator("div.tree-label", has_text="Hard Discount").click()
    print(f"Hard Discount have been selected.")


    target_frame.click("button.primary.ng-binding:has-text('Apply')")
    target_frame.click("div#runButton[alt='Run Report']")
    time.sleep(30)
    
    
    
    Hard_Discount_ytd_data = fetch_totals_data()
    print('the 2nd extraction for Hard Discount is done')
     
 #=================================get the custom date report====================================
 #=================================Super market====================================

    target_frame.wait_for_selector("#quickDateRangeSelect", timeout=60000)
    target_frame.select_option("#quickDateRangeSelect", value="-1")
    
    
    target_frame.wait_for_selector("#calendarListRangeBeginTrigger", state="visible", timeout=30000)
    time.sleep(2)
    target_frame.fill("#calendarListRangeBeginTrigger", '01/04/26')
    time.sleep(2)
    # print(f"start at 01/04/26")

    today = datetime.today()
    days_since_saturday = (today.weekday() - 5) % 7
    last_saturday = today - timedelta(days=days_since_saturday)
    last_saturday_str = last_saturday.strftime("%m/%d/%y")
    target_frame.wait_for_selector("#calendarListRangeEndTrigger", state="visible", timeout=30000)
    target_frame.fill("#calendarListRangeEndTrigger", last_saturday_str)
    #print(f"end at {last_saturday_str}")
    target_frame.click("body")

    target_frame.wait_for_selector("#dummySelector", timeout=60000)
    target_frame.click("#dummySelector")
    target_frame.click("div.ng-binding.ng-scope:has-text('Brand Hierarchy')")
    target_frame.locator("div#dummySelector", has_text="Single-Level").click()
    target_frame.locator("div.ng-binding.ng-scope", has_text="Multi-Level").click()
    target_frame.click("i.tree-branch-head")

    #select hard Super_Market
    for brand in Super_Market:
        target_frame.locator("div.tree-label", has_text=brand).click()
    
    target_frame.click("button.primary.ng-binding:has-text('Apply')")       
    target_frame.click("div#runButton[alt='Run Report']")
    print('the first extraction for SuperMarket is runing')
    ##🚨since there is a web bug, the click steps should be repeated to get the correct data
    
    # iframe_element = page.query_selector("iframe[src*='/report/app']")
    # if not iframe_element:
    #     raise Exception("iframe missing")

    # target_frame = iframe_element.content_frame()
    # if not target_frame:
    #     raise Exception("iframe not loaded yet")
    
    try:
        # ensure target_frame 
        if not target_frame:
            raise Exception("target_frame is None, iframe not ready")
        # wait selection-list disappear
        target_frame.wait_for_selector("div#selection-list", state="hidden", timeout=180000)
        print("selection-list disappeared, fetching textAnalyticsGrid table...")

        # wait the table has data
        target_frame.wait_for_function(
            """() => {
                const table = document.querySelector('table#textAnalyticsGrid');
                return table && table.querySelectorAll('tbody tr').length > 0;
            }""",
            timeout=30000
        )
    except Exception as e:
        print("selection-list did not disappear or table not found:", e)
    
    
    
    # select the costom date option
    target_frame.wait_for_selector("#quickDateRangeSelect", timeout=60000)
    target_frame.select_option("#quickDateRangeSelect", value="-1")
    # input '01/04/26'
    target_frame.wait_for_selector("#calendarListRangeBeginTrigger", state="visible", timeout=60000)
    time.sleep(2)
    target_frame.fill("#calendarListRangeBeginTrigger", '01/04/26')
    time.sleep(2)
    print(f"start at 01/04/26")


    target_frame.wait_for_selector("#calendarListRangeEndTrigger", state="visible", timeout=60000)
    target_frame.fill("#calendarListRangeEndTrigger", last_saturday_str)
    print(f"end at {last_saturday_str}")
    target_frame.click("body")

    target_frame.wait_for_selector("#dummySelector", timeout=60000)
    target_frame.click("#dummySelector")
    target_frame.click("div.ng-binding.ng-scope:has-text('Brand Hierarchy')")
    target_frame.locator("div#dummySelector", has_text="Single-Level").click()
    target_frame.locator("div.ng-binding.ng-scope", has_text="Multi-Level").click()
    target_frame.click("i.tree-branch-head")

    
    for brand in Super_Market:
        target_frame.locator("div.tree-label", has_text=brand).click()
    print(f"Super Market have been selected.")


    target_frame.click("button.primary.ng-binding:has-text('Apply')")
    target_frame.click("div#runButton[alt='Run Report']")
    time.sleep(30)
    
    
    
    Super_Market_ytd_data = fetch_totals_data()
    print('the 2nd extraction for Super Market is done')
    

    
 #===================export==============================
    
def create_pivot(last_wk, last_4wk, last_12wk, ytd):
    # Set the 'Date Range' column for each DataFrame
    last_wk['Date Range'] = 'Last Week'
    last_4wk['Date Range'] = 'Last 4 Weeks'
    last_12wk['Date Range'] = 'Last 12 Weeks'
    ytd['Date Range'] = 'Year to Date'
    
    # Define the desired column order
    columns_order = ['Date Range','In-Store OSAT','Good value for money (Top Box %)','Items In Stock (Top Box %)']
    
    # reorder columns based on order
    def reorder_columns(df):
        cols_existing = [c for c in columns_order if c in df.columns]
        return df[cols_existing]
    
    # Reorder columns in each DataFrame
    last_wk = reorder_columns(last_wk)
    last_4wk = reorder_columns(last_4wk)
    last_12wk = reorder_columns(last_12wk)
    ytd = reorder_columns(ytd)
    
    # Concatenate all DataFrames vertically
    all_data = pd.concat([last_wk, last_4wk, last_12wk, ytd], ignore_index=True)
    
    # Melt the DataFrame from wide to long format and then pivot
    pivot_df = (
        all_data
        .melt(
            id_vars='Date Range',
            value_vars=['In-Store OSAT','Good value for money (Top Box %)','Items In Stock (Top Box %)'],
            var_name='Metric',
            value_name='Value'
        )
        .pivot(index='Metric', columns='Date Range', values='Value')
    )
    
    # Reorder the columns
    desired_columns_order = ['Last Week', 'Last 4 Weeks', 'Last 12 Weeks', 'Year to Date']
    pivot_df = pivot_df[desired_columns_order]
    
    # Reorder the rows to sequence
    desired_row_order = ['In-Store OSAT','Good value for money (Top Box %)','Items In Stock (Top Box %)']
    pivot_df = pivot_df.reindex(desired_row_order)
    
    return pivot_df

Super_Market_data_pivot = create_pivot(
Super_Market_LastWk_data,
Super_Market_Last4Wk_data,
Super_Market_Last12Wk_data,
Super_Market_ytd_data)

Hard_Discount_data_pivot = create_pivot(
Hard_Discount_LastWk_data,
Hard_Discount_Last4Wk_data,
Hard_Discount_Last12Wk_data,
Hard_Discount_ytd_data)


today_str = datetime.today().strftime('%Y-%m-%d')

# Set base export directory and today's folder
base_dir = r"C:\Users\sopfei\Desktop\Weekly Deck\Export"
today_dir = os.path.join(base_dir, today_str)

# Create the folder
os.makedirs(today_dir, exist_ok=True)

# Excel file path
output_file = os.path.join(today_dir, f'OSAT_export_{today_str}.xlsx')

# pivot tables to Excel
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    # Super Market sheet
    Super_Market_data_pivot.to_excel(writer, sheet_name='Super_Market')
    # Auto-adjust column widths
    for i, col in enumerate(Super_Market_data_pivot.columns):
        col_len = max(Super_Market_data_pivot[col].astype(str).map(len).max(), len(col)) + 2
        writer.sheets['Super_Market'].set_column(i+1, i+1, col_len)
    
    # Hard Discount sheet
    Hard_Discount_data_pivot.to_excel(writer, sheet_name='Hard_Discount')
    # Auto-adjust column widths
    for i, col in enumerate(Hard_Discount_data_pivot.columns):
        col_len = max(Hard_Discount_data_pivot[col].astype(str).map(len).max(), len(col)) + 2
        writer.sheets['Hard_Discount'].set_column(i+1, i+1, col_len)

print(f"tables exported successfully to {output_file}")
    
    
    
    
    
    
    # browser.close()
    
    