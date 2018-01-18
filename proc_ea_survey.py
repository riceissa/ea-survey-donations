#!/usr/bin/env python3

import csv
import logging
import mysql.connector


logging.basicConfig(level=logging.DEBUG)


years = [
        2015,
        2016,
        ]

# From
# https://github.com/peterhurford/ea-data/blob/e2155246f2919b9f94c2c169423bf29b05a4aa16/models/2017/easurvey/variable_names.R#L62-L109
charities = [
        {"name": "Rethink Charity", "var": "donate_RC"},
        {"name": "80000 Hours", "var": "donate_80K"},
        {"name": "Against Malaria Foundation", "var": "donate_amf"},
        {"name": "Animal Charity Evaluators", "var": "donate_ace"},
        {"name": "Center for Applied Rationality", "var": "donate_cfar"},
        {"name": "Centre for Effective Altruism", "var": "donate_cea"},
        {"name": "Charity Science", "var": "donate_cs"},
        {"name": "Deworm the World Initiative", "var": "donate_dtw"},
        {"name": "Effective Altruism Foundation", "var": "donate_ef"},
        {"name": "END Fund", "var": "donate_end"},
        {"name": "Faunalytics", "var": "donate_faunalytics"},
        {"name": "Foundational Research Institute", "var": "donate_fri"},
        {"name": "Future of Humanity Institute", "var": "donate_fhi"},
        {"name": "GiveDirectly", "var": "donate_gd"},
        {"name": "GiveWell", "var": "donate_gw"},
        {"name": "The Good Food Institute", "var": "donate_gf"},
        {"name": "Machine Intelligence Research Institute",
            "var": "donate_miri"},
        {"name": "Malaria Consortium", "var": "donate_mc"},
        {"name": "Mercy For Animals", "var": "donate_mfa"},
        {"name": "Schistosomiasis Control Initiative", "var": "donate_sci"},
        {"name": "Sightsavers", "var": "donate_sightsavers"},
        {"name": "Sentience Politics", "var": "donate_sp"},
        {"name": "The Humane League", "var": "donate_thl"},
        {"name": "The Life You Can Save", "var": "donate_tlycs"},
        ]

# These are the donors that we already track fully in the database, so don't
# generate insert lines for them
ignored_donors = {
        "Peter Hurford",
        "Michael Dickens",
        "Patrick Brinich-Langlois",
        "Gordon Irlam"
        }


cnx = mysql.connector.connect(user='issa', database='donations')
cursor = cnx.cursor()

cursor.execute("""select distinct(donor) from donations""")
existing_donors = {x[0] for x in cursor}

cursor.execute("""select donee,cause_area from donees
               where cause_area is not NULL""")
causes = {name: cause for name, cause in cursor}

cursor.close()
cnx.close()


def mysql_quote(x):
    '''
    Quote the string x using MySQL quoting rules. If x is the empty string,
    return "NULL". Probably not safe against maliciously formed strings, but
    whatever; our input is fixed and from a basically trustable source..
    '''
    if not x:
        return "NULL"
    x = x.replace("\\", "\\\\")
    x = x.replace("'", "''")
    x = x.replace("\n", "\\n")
    return "'{}'".format(x)


print("""insert into donations(donor, donee, amount, fraction, donation_date,
    donation_date_precision, donation_date_basis, cause_area, url, notes,
    payment_modality, match_eligible, goal_amount, influencer, employer_match,
    matching_employer, amount_original_currency, original_currency,
    currency_conversion_date, currency_conversion_basis) values""")

with open("2017-ea-survey-sharable-data.csv", newline='') as f:
    reader = csv.DictReader(f)
    first = True
    for row in reader:
        logging.info("Permission for %s: %s, %s, %s", row['full_name'],
                     row['can_share_2015_donations'],
                     row['can_share_2016_donations'], row['can_share'])
        if row['full_name'] in existing_donors:
            logging.info("%s is already in the database", row['full_name'])
        for charity in charities:
            for year in years:
                donation_var = charity['var'] + "_" + str(year) + "_c"
                # Flag values that cannot be converted to float
                if (donation_var in row and row[donation_var] and
                        row[donation_var] != "NA"):
                    try:
                        float(row[donation_var])
                    except ValueError as e:
                        logging.info("%s: for %s, for var %s, the value is %s",
                                     e, row['full_name'], donation_var,
                                     row[donation_var])
                if (donation_var in row and row[donation_var] and
                        row[donation_var] != "NA"):
                    amount = float(row[donation_var].replace(",", ""))
                else:
                    amount = 0
                if amount and row['full_name'] not in ignored_donors:
                    print("    " + ("" if first else ",") + "(" + ",".join([
                        mysql_quote(row['full_name']),
                        mysql_quote(charity['name']),
                        str(amount),
                        "NULL",
                        mysql_quote(str(year) + "-01-01"),
                        mysql_quote("year"),
                        mysql_quote("Effective Altruism Survey"),
                        mysql_quote(causes[charity['name']]),
                        mysql_quote(
                            "https://github.com/peterhurford/ea-data/"),
                        "NULL",
                        "NULL",
                        "NULL",
                        "NULL",
                        "NULL",
                        "NULL",
                        "NULL",
                        str(float(row[donation_var[:-len("_c")]])),  # amount original
                        mysql_quote(row['currency']),  # original currency
                        mysql_quote("2017-08-05"),
                        mysql_quote("Fixer.io"),
                    ]) + ")")
                    first = False

print(";")
