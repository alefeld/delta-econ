import collections
import json
import requests
import statistics

import gspread

spreadsheet_id = '1gWt4dGGDhxZtSNDXnwXDmAB79sp4LyGhIq2L5SKEBU0'

# Connect to spreadsheet
credentials = gspread.service_account()
worksheet = credentials.open_by_key(spreadsheet_id).worksheet('Bet Data')

# Get player stars
print("Getting player stars...")
playerratings = collections.defaultdict(dict)
response = requests.get('https://api2.sibr.dev/mirror/players')
players = json.loads(response.content)
n_players = 0
for player in players:
    n_players += 1
    player_id = player['id']
    attributes = {}
    for attribute in player['attributes']:
        attributes[attribute['name']] = attribute['value']
    avg_batting = statistics.mean([attributes[attribute] for attribute in ['Sight','Thwack','Ferocity']])
    avg_running = statistics.mean([attributes[attribute] for attribute in ['Dodge','Hustle','Ferocity']])
    avg_defense = statistics.mean([attributes[attribute] for attribute in ['Magnet','Reach','Reflex']])
    avg_pitching = statistics.mean([attributes[attribute] for attribute in ['Control','Stuff','Guile']])
    avg_vibes = statistics.mean([attributes[attribute] for attribute in ['Drama','Survive','Thrive']])
    playerratings[player_id]['batting'] = avg_batting
    playerratings[player_id]['running'] = avg_running
    playerratings[player_id]['defense'] = avg_defense
    playerratings[player_id]['pitching'] = avg_pitching
    playerratings[player_id]['vibes'] = avg_vibes


# Get records and calculate team ratings
print("Getting team records and ratings...")
more_teams = True
url_base = 'https://api2.sibr.dev/chronicler/v0/versions?kind=team&order=asc'
url = url_base
records = collections.defaultdict(dict)
teamratings = collections.defaultdict(lambda: collections.defaultdict(dict))
timestamp_last = ''
n_teams = 0
while more_teams:
    # Load
    response = requests.get(url)
    teams = json.loads(response.content)['items']

    # Loop over each object
    for team in teams:
        n_teams += 1
        timestamp_last = team['valid_from']
        # Get team data
        teamdata = team['data']
        if teamdata['activeTeam']:
            teamid = teamdata['id']
            wins = teamdata['standings'][0]['wins']
            losses = teamdata['standings'][0]['losses']
            day = wins + losses
            records[teamid][day] = (wins,losses)

            # This is bugged data!! Do not use!
            # # 4 categories: batting, running, defense, pitching
            # for rating in teamdata['categoryRatings']:
            #     teamratings[teamid][day][rating['name']] = rating['stars']

            lineup_batting = []
            lineup_running = []
            lineup_defense = []
            rotation_pitching = []
            team_vibes = []
            for player in teamdata['roster']:
                player_id = player['id']
                if player['rosterSlots'][0]['active']:
                    if player['rosterSlots'][0]['location'] == 'LINEUP':
                        lineup_batting.append(playerratings[player_id]['batting'])
                        lineup_running.append(playerratings[player_id]['running'])
                        lineup_defense.append(playerratings[player_id]['defense'])
                        team_vibes.append(playerratings[player_id]['vibes'])
                    if player['rosterSlots'][0]['location'] == 'ROTATION':
                        rotation_pitching.append(playerratings[player_id]['pitching'])
                        team_vibes.append(playerratings[player_id]['vibes'])
            teamratings[teamid][day]['batting'] = statistics.mean(lineup_batting)
            teamratings[teamid][day]['running'] = statistics.mean(lineup_running)
            teamratings[teamid][day]['defense'] = statistics.mean(lineup_defense)
            teamratings[teamid][day]['pitching'] = statistics.mean(rotation_pitching)
            teamratings[teamid][day]['vibes'] = statistics.mean(team_vibes)


    # If this isn't the last bit of data, make a new url and continue
    print(timestamp_last)
    if timestamp_last in url:
        more_teams = False
    else:
        url = url_base + f'&after={timestamp_last}'

# Get game odds (last available odds for that game)
print("Getting game odds and winners...")
n_games = 0
more_games = True
timestamp_last = ''
url_base = 'https://api2.sibr.dev/chronicler/v0/versions?kind=game_bet_data&order=asc'
url = url_base
sheet_data = []
gameids_processed = []
while more_games:
    # Load
    response = requests.get(url)
    games = json.loads(response.content)['items']

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

        day = home_wins + home_losses + 1

        # Don't process a game if we already processed the complete version
        # This prevents overwriting the first complete record with a later one 
        if game_id not in gameids_processed:
            if gamedata['complete']:

                day = gamedata['homeTeamInfo']['wins'] + gamedata['homeTeamInfo']['losses']
            
                home_odds = gamedata['homeTeamBetData']['currentOdds']
                away_odds = gamedata['awayTeamBetData']['currentOdds']
                
                home_pitcher_id = gamedata['homeTeamInfo']['pitcher']['id']
                home_pitcher_rating = playerratings[home_pitcher_id]['pitching']
                away_pitcher_id = gamedata['awayTeamInfo']['pitcher']['id']
                away_pitcher_rating = playerratings[away_pitcher_id]['pitching']

                if gamedata['awayScore']>gamedata['homeScore']:
                    winner = "Away"
                else:
                    winner = "Home"
                
                # Add data to spreadsheet payload
                sheet_data.append([game_id, day,
                                   home_id, teamratings[home_id][day-1]['batting'], teamratings[home_id][day-1]['running'], teamratings[home_id][day-1]['defense'], teamratings[home_id][day-1]['pitching'], teamratings[home_id][day-1]['vibes'],
                                   home_pitcher_rating, home_odds, records[home_id][day-1][0], records[home_id][day-1][1],
                                   away_id, teamratings[away_id][day-1]['batting'], teamratings[away_id][day-1]['running'], teamratings[away_id][day-1]['defense'], teamratings[away_id][day-1]['pitching'], teamratings[away_id][day-1]['vibes'],
                                   away_pitcher_rating, away_odds, records[away_id][day-1][0], records[away_id][day-1][1], winner])
                gameids_processed.append(game_id)

    # If this isn't the last bit of data, make a new url and continue
    print(timestamp_last)
    if timestamp_last in url:
        more_games = False
    else:
        url = url_base + f'&after={timestamp_last}'

# Log and update
print(f"{n_players} player objects processed")
print(f"{n_teams} team objects processed")
print(f"{n_games} game objects processed")
worksheet.update('A2:W', sheet_data)