
from datetime import datetime

# Added task for Tableau maintenance on third tuesday 8-11pm
def tableau_skip(**_):

    # Get today's date
    today = datetime.today()
    now = datetime.now()

    # Print in default ISO format (YYYY-MM-DD)
    weekday = today.strftime("%A")
    month = today.strftime("%B")
    day = today.strftime("%d")
    print(f"Today is {weekday},{month} {day}")
    print(now.strftime("%H:%M:%S"))
    ###

    #Is it tuesday?
    is_tuesday = (today.strftime("%w") == 2)

    #Is it the 3rd week?
    is_third_week = (15 <= int(day) <= 21)
    #is it in time window?
    start_window = 20  # 8 PM
    end_window = 23 # 11 PM
    in_time_window = start_window <= int(now.strftime("%H")) <= end_window

    if is_tuesday and is_third_week and in_time_window:
        print("skip_tableau")
    else:
        print("tableau_tasks")

    # if is_tuesday and is_third_week and in_time_window:
    #     return "skip_tableau"
    # else:
    #     return "tableau_tasks"
tableau_skip()