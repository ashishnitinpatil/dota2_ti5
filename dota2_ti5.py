#-------------------------------------------------------------------------------
# Name:        Tinkering with the TI5 matches data
# Purpose:     Fun... imba fun :D
#
# Author:      Ashish Nitin Patil
#
# Created:     02/08/2015
# Copyright:   (c) Ashish 2015
# Licence:     New BSD License
#-------------------------------------------------------------------------------

from __future__ import print_function

from dota2py import api
from dota2api_scripts import match_history, match_details
from pymongo import MongoClient
from time import ctime, sleep
import os
import csv

# Connect to MongoDB and authenticate to the Dota2 database
client = MongoClient()
db = client.dota2
db.authenticate('dota2', 'dota2')

# TODO
# Fetch all matches through match history and then update db.ti5_matches

# get all match ids for ti5 present in the database
ti5_matches_mongo = db.ti5_matches
ti5_match_details = db.ti5_match_details
all_match_ids_mongo = list(ti5_matches_mongo.distinct('match_id'))
match_ids_details_fetched = list(ti5_match_details.distinct('match_id'))
# Match ids for which we need to fetch details
match_ids = list(set(all_match_ids_mongo) - set(match_ids_details_fetched))

# set api key
api.set_api_key("06A0619C384CFA31B4D956A805743BEF")
# fetch all ti5 matches till date (league id - 2733)
# ti5_matches = match_history.latest_matches(league_id=2733)
# collect all the match ids
# all_match_ids = set(x['match_id'] for x in ti5_matches['matches'])

# fetch existing match details from mongo
all_match_details = {}
for match_id in match_ids_details_fetched:
    all_match_details[match_id] = ti5_match_details.find({"match_id": match_id})[0]

# fetch match details for derived match_ids
# trying to do this in chunks now, since bulk fetches failed multiple times
to_remove = []
i = 0
iteration = 1
while match_ids:
    print(ctime(), "Iteration 1 - Matches remaining to be fetched", len(match_ids))
    for match_id in match_ids:
        try:
            cur_response = api.get_match_details(match_id=match_id)
            if not 'match_id' in cur_response['result']:
                print("Match id", match_id, ":", "Unsuccessful fetch / bad id")
            else:
                # successful data fetch
                all_match_details[match_id] = cur_response['result']
                to_remove.append(match_id)
            print("Matches fetched = #", len(all_match_details), sep="")
        except:
            print("Match id", match_id, ":", "Unsuccessful fetch / bad id")
        sleep(3)
    for match_id in to_remove:
        match_ids.remove(match_id)
    iteration += 1

# fetch all the match times (local time, local timezone)
match_times = [v['start_time'] for k,v in all_match_details.items()]
print(ctime(min(match_times)), "to", ctime(max(match_times)))

# fetch team ids and team names of all participating teams
all_teams = {v['dire_team_id']:v['dire_name'] for k,v in all_match_details.items()}
all_teams.update({v['radiant_team_id']:v['radiant_name'] for k,v in all_match_details.items()})

# match time - 1438013558 corresponds to 1st group stage match (july 27th)
group_stage_match_details = {
    k:v for k,v in all_match_details.items()
        if v['start_time'] >= 1438013558
}

# fetch team ids and team names of all qualified teams (16)
teams = {
    v['dire_team_id']:v['dire_name'] for k,v in group_stage_match_details.items()
}
teams.update({
    v['radiant_team_id']:v['radiant_name'] for k,v in group_stage_match_details.items()
})

# general team-match level stats
team_match_stats = []
for match_id in group_stage_match_details:
    cur_match = group_stage_match_details[match_id]
    team_match_stats.append([
        cur_match['radiant_team_id'], # team id
        cur_match['radiant_name'],    # team name
        'radiant',                    # side
        1 if cur_match['radiant_win'] else 0, # win/loss
        cur_match['first_blood_time'],# fb time
        cur_match['duration'],        # duration
        cur_match['positive_votes'],  # positive votes
        cur_match['negative_votes']   # negative votes
    ])
    team_match_stats.append([
        cur_match['dire_team_id'],    # team id
        cur_match['dire_name'],       # team name
        'dire',                       # side
        0 if cur_match['radiant_win'] else 1, # win/loss
        cur_match['first_blood_time'],# fb time
        cur_match['duration'],        # duration
        cur_match['positive_votes'],  # positive votes
        cur_match['negative_votes']   # negative votes
    ])
# write to csv
with open("F:\\Projects\\dota2_ti5\\team_match_stats.csv", "wb") as cur_file:
    csv_writer = csv.writer(cur_file)
    csv_writer.writerow(
        ["team_id", "team_name", "side", "win", "first_blood_time",
         "total_match_time", "positive_votes", "negative_votes"]
    )
    csv_writer.writerows(team_match_stats)
