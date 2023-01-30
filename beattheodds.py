import argparse
import collections
import datetime
import json
import requests
import statistics

import gspread

spreadsheet_ids = {
    1: '1gWt4dGGDhxZtSNDXnwXDmAB79sp4LyGhIq2L5SKEBU0',
    2: '1Qj4B0DAlwXmrVZSNXm5lyfgWybfrIb5km4oN6aBCt1c',
}

season_ids = {
    1: 'cd1b6714-f4de-4dfc-a030-851b3459d8d1',
    2: '7af53acf-1fb9-40e8-96c7-ab8308a353f9',
}

timestamps_start = {
    1: '2023-01-08T10:00:00Z',
    2: '2023-01-22T17:00:00Z',
}

# Set up arguments
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('season',
                            type = int,
                            help = "Season number to process (1-indexed).")
args = arg_parser.parse_args()

# Connect to spreadsheet
credentials = gspread.service_account()
worksheet = credentials.open_by_key(spreadsheet_ids[args.season]).worksheet('Bet Data')

# Get gameday start times
print("Getting game start times...")
url = f'https://api2.sibr.dev/chronicler/v0/entities?kind=game'
data = json.loads(requests.get(url).content)
games = data['items']
games_start = {}
games_day = {}
for game in games:
    if game['data']['seasonId'] == season_ids[args.season]:
        # game_datetime = datetime.datetime.strptime(game['data']['startTime'], "%Y-%m-%dT%H:%M:%S.%fZ")
        game_datetime = game['data']['startTime']
        games_start[game['data']['day']+1] = game_datetime
        game_id = game['entity_id']
        games_day[game_id] = game['data']['day']+1

# Get player stars
print("Getting Players...")
url_base = f'https://api2.sibr.dev/chronicler/v0/versions?kind=player&order=asc'
url = url_base
more_players = True
playerratings = collections.defaultdict(list)
while more_players:
    # Load
    data = json.loads(requests.get(url).content)
    players = data['items']
    next_page = data['next_page']

    for player in players:
        valid_from = player['valid_from']
        valid_to = player['valid_to']

        player_id = player['data']['id']
        attributes = {}
        for attribute in player['data']['attributes']:
            attributes[attribute['name']] = attribute['value']
        avg_batting = statistics.mean([attributes[attribute] for attribute in ['Sight','Thwack','Ferocity']])
        avg_running = statistics.mean([attributes[attribute] for attribute in ['Dodge','Hustle','Stealth']])
        avg_defense = statistics.mean([attributes[attribute] for attribute in ['Magnet','Reach','Reflex']])
        avg_pitching = statistics.mean([attributes[attribute] for attribute in ['Control','Stuff','Guile']])
        avg_vibes = statistics.mean([attributes[attribute] for attribute in ['Drama','Survive','Thrive']])
        ratings = {
            'valid_from': valid_from,
            'batting': avg_batting,
            'running': avg_running,
            'defense': avg_defense,
            'pitching': avg_pitching,
            'vibes': avg_vibes
        }
        playerratings[player_id].append(ratings)

    # If this isn't the last bit of data, make a new url and continue
    if next_page:
        url = url_base + f'&page={next_page}'
    else:
        more_players = False

# Get records and calculate team ratings
# Be clever... sort DESCENDING and save by day.
# Therefore each day will be saved with data for the very end of the previous game.
print("Getting team records and calculating ratings from players...")
more_teams = True
url_base = f'https://api2.sibr.dev/chronicler/v0/versions?kind=team&order=desc&after={timestamps_start[args.season]}'
url = url_base
records = collections.defaultdict(dict)
teamratings = collections.defaultdict(lambda: collections.defaultdict(dict))
timestamp_last = ''
n_teams = 0
while more_teams:
    # Load
    data = json.loads(requests.get(url).content)
    teams = data['items']
    next_page = data['next_page']

    # Loop over each object
    for team in teams:

        n_teams += 1
        valid_from = team['valid_from']
        valid_to = team['valid_to']

        timestamp_last = team['valid_from']
        # Get team data
        teamdata = team['data']
        if teamdata['activeTeam']:
            # Unfortunately there's no way to request a specific season, so this will have to do
            if team['data']['standings'][0]['seasonId'] != season_ids[args.season]:
                continue

            teamid = teamdata['id']
            wins = teamdata['standings'][0]['wins']
            losses = teamdata['standings'][0]['losses']
            day = wins + losses + 1 # This team makeup is valid for the start of the *next* day's games.
            records[teamid][day] = (wins,losses)

            lineup_batting = []
            lineup_running = []
            lineup_defense = []
            rotation_pitching = []
            team_vibes = []
            for player in teamdata['roster']:
                player_id = player['id']
                if player['rosterSlots'][0]['active']:
                    # If only one version of a player, use that one
                    if len(playerratings[player_id]) == 1:
                        player_ratings = playerratings[player_id][0]
                    # If multiple versions, go chronologically (regular order) and overwrite every time the version began before the game start
                    else:
                        game_start = datetime.datetime.strptime(games_start[day], "%Y-%m-%dT%H:%M:%S.%fZ")
                        for version in playerratings[player_id]:
                            if datetime.datetime.strptime(version['valid_from'], "%Y-%m-%dT%H:%M:%S.%fZ") < game_start:
                                player_ratings = version
                    if player['rosterSlots'][0]['location'] == 'LINEUP':
                        lineup_batting.append(player_ratings['batting'])
                        lineup_running.append(player_ratings['running'])
                        lineup_defense.append(player_ratings['defense'])
                        team_vibes.append(player_ratings['vibes'])
                    if player['rosterSlots'][0]['location'] == 'ROTATION':
                        rotation_pitching.append(player_ratings['pitching'])
                        team_vibes.append(player_ratings['vibes'])
            teamratings[teamid][day]['batting'] = statistics.mean(lineup_batting)
            teamratings[teamid][day]['running'] = statistics.mean(lineup_running)
            teamratings[teamid][day]['defense'] = statistics.mean(lineup_defense)
            teamratings[teamid][day]['pitching'] = statistics.mean(rotation_pitching)
            teamratings[teamid][day]['vibes'] = statistics.mean(team_vibes)

    # If this isn't the last bit of data, make a new url and continue
    if next_page:
        url = url_base + f'&page={next_page}'
    else:
        more_teams = False

# Get game odds (last available odds for that game)
print("Getting game odds and winners...")
n_games = 0
more_games = True
timestamp_last = ''
url_base = f'https://api2.sibr.dev/chronicler/v0/versions?kind=game_bet_data&order=asc&after={timestamps_start[args.season]}'
url = url_base
sheet_data = []
gameids_processed = []
while more_games:
    # Load
    data = json.loads(requests.get(url).content)
    games = data['items']
    next_page = data['next_page']

    # Loop over games
    for game in games:
        n_games += 1
        timestamp_last = game['valid_from']
        gamedata = game['data']

        game_id = gamedata['gameId']

        home_id = gamedata['homeTeamInfo']['teamId']
        home_wins = gamedata['homeTeamInfo']['wins']
        home_losses = gamedata['homeTeamInfo']['losses']
        away_id = gamedata['awayTeamInfo']['teamId']
        away_wins = gamedata['awayTeamInfo']['wins']
        away_losses = gamedata['awayTeamInfo']['losses']

        # Don't process a game if we already processed the complete version
        # This prevents overwriting the first complete record with a later one 
        if game_id not in gameids_processed:
            if gamedata['complete']:

                day = games_day[game_id]

                # Ignore postseason
                if day > 90:
                    continue

                # FIXME Season 2 Temporal Anomoly Handling
                if (day not in teamratings[home_id]) or (day not in teamratings[away_id]):
                    continue
            
                home_odds = gamedata['homeTeamBetData']['currentOdds']
                away_odds = gamedata['awayTeamBetData']['currentOdds']
                
                home_pitcher_id = gamedata['homeTeamInfo']['pitcher']['id']
                # If only one version of a player, use that one
                if len(playerratings[home_pitcher_id]) == 1:
                    home_pitcher_ratings = playerratings[home_pitcher_id][0]
                # If multiple versions, go chronologically (regular order) and overwrite every time the version began before the game start
                else:
                    game_start = datetime.datetime.strptime(games_start[day], "%Y-%m-%dT%H:%M:%S.%fZ")
                    for version in playerratings[home_pitcher_id]:
                        if datetime.datetime.strptime(version['valid_from'], "%Y-%m-%dT%H:%M:%S.%fZ") < game_start:
                            home_pitcher_ratings = version
                home_pitcher_rating = home_pitcher_ratings['pitching']

                away_pitcher_id = gamedata['awayTeamInfo']['pitcher']['id']
                # If only one version of a player, use that one
                if len(playerratings[away_pitcher_id]) == 1:
                    away_pitcher_ratings = playerratings[away_pitcher_id][0]
                # If multiple versions, go chronologically (regular order) and overwrite every time the version began before the game start
                else:
                    game_start = datetime.datetime.strptime(games_start[day], "%Y-%m-%dT%H:%M:%S.%fZ")
                    for version in playerratings[away_pitcher_id]:
                        if datetime.datetime.strptime(version['valid_from'], "%Y-%m-%dT%H:%M:%S.%fZ") < game_start:
                            away_pitcher_ratings = version
                away_pitcher_rating = away_pitcher_ratings['pitching']

                if gamedata['awayScore']>gamedata['homeScore']:
                    winner = "Away"
                else:
                    winner = "Home"
                
                # Add data to spreadsheet payload
                sheet_data.append([game_id, day,
                                   home_id, teamratings[home_id][day]['batting'], teamratings[home_id][day]['running'], teamratings[home_id][day]['defense'], teamratings[home_id][day]['pitching'], teamratings[home_id][day]['vibes'],
                                   home_pitcher_rating, home_odds, records[home_id][day][0], records[home_id][day][1],
                                   away_id, teamratings[away_id][day]['batting'], teamratings[away_id][day]['running'], teamratings[away_id][day]['defense'], teamratings[away_id][day]['pitching'], teamratings[away_id][day]['vibes'],
                                   away_pitcher_rating, away_odds, records[away_id][day][0], records[away_id][day][1], winner])
                gameids_processed.append(game_id)

    # If this isn't the last bit of data, make a new url and continue
    print(timestamp_last)
    if next_page:
        url = url_base + f'&page={next_page}'
    else:
        more_games = False

# Log and update
print(f"{n_teams} team objects processed")
print(f"{n_games} game objects processed")
worksheet.update('A2:W', sheet_data)