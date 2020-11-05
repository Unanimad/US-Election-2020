import pandas as pd
import requests

from apscheduler.schedulers.blocking import BlockingScheduler
from kaggle import KaggleApi


def main():
    request = requests.get('https://graphics.thomsonreuters.com/2020-US-elex/20201103/20201103-county.json')
    county = request.json()

    last_update = county['timestamp']

    president = county['P']
    senate = county['S']
    house = county['S2']
    governors = county['G']

    president_state = []
    president_county = []
    president_county_candidate = []

    for state in president.keys():
        for i, county in enumerate(president[state].keys()):
            if i == 0:
                president_state.append([
                    president[state][county][0][0],
                    president[state][county][0][1],
                    president[state][county][0][3]
                ])
            else:
                for x, candidate in enumerate(president[state][county][1]):
                    if x == 0:
                        president_county.append([
                            president_state[-1][0],
                            president[state][county][0][0],
                            president[state][county][0][1],
                            president[state][county][0][2],
                            president[state][county][0][3]])
                    else:
                        president_county_candidate.append([
                            president_state[-1][0],
                            president[state][county][0][0],
                            ' '.join(candidate[:2]),
                            candidate[2],
                            candidate[4]])

    president_state_df = pd.DataFrame(president_state, columns=['state', 'votes', 'electoral_vote'])
    president_county_df = pd.DataFrame(president_county,
                                       columns=['state', 'county', 'current_votes', 'total_votes', 'percent'])
    president_county_candidate_df = pd.DataFrame(president_county_candidate,
                                                 columns=['state', 'county', 'candidate', 'party', 'votes'])

    president_state_df.to_csv('data/president_state.csv', index=False)
    president_county_df.to_csv('data/president_county.csv', index=False)
    president_county_candidate_df.to_csv('data/president_county_candidate.csv', index=False)

    senate_state = []
    senate_county = []
    senate_county_candidate = []

    for state in senate.keys():
        for i, county in enumerate(senate[state].keys()):
            if i == 0:
                senate_state.append([
                    senate[state][county][0][0],
                    senate[state][county][0][1],
                    senate[state][county][0][3]
                ])
            else:
                for x, candidate in enumerate(senate[state][county][1]):
                    if x == 0:
                        senate_county.append([
                            senate_state[-1][0],
                            senate[state][county][0][0],
                            senate[state][county][0][1],
                            senate[state][county][0][2],
                            senate[state][county][0][3]
                        ])
                    else:
                        senate_county_candidate.append([
                            senate_state[-1][0],
                            senate[state][county][0][0],
                            ' '.join(candidate[:2]),
                            candidate[2],
                            candidate[4]
                        ])

    senate_state_df = pd.DataFrame(senate_state, columns=['state', 'votes', 'electoral_vote'])
    senate_county_df = pd.DataFrame(senate_county,
                                    columns=['state', 'county', 'current_votes', 'total_votes', 'percent'])
    senate_county_candidate_df = pd.DataFrame(senate_county_candidate,
                                              columns=['state', 'county', 'candidate', 'party', 'votes'])

    senate_state_df.to_csv('data/senate_state.csv', index=False)
    senate_county_df.to_csv('data/senate_county.csv', index=False)
    senate_county_candidate_df.to_csv('data/senate_county_candidate.csv', index=False)

    house_state = []
    house_county = []
    house_county_candidate = []

    api = KaggleApi()
    api.authenticate()

    for state in house.keys():
        for i, county in enumerate(house[state].keys()):
            if i == 0:
                house_state.append([
                    house[state][county][0][0],
                    house[state][county][0][1],
                    house[state][county][0][3]
                ])
            else:
                for x, candidate in enumerate(house[state][county][1]):
                    if x == 0:
                        house_county.append([
                            house_state[-1][0],
                            house[state][county][0][0],
                            house[state][county][0][1],
                            house[state][county][0][2],
                            house[state][county][0][3]
                        ])
                    else:
                        house_county_candidate.append([
                            house_state[-1][0],
                            house[state][county][0][0],
                            ' '.join(candidate[:2]),
                            candidate[2],
                            candidate[4]
                        ])

    house_state_df = pd.DataFrame(house_state, columns=['state', 'votes', 'electoral_vote'])
    house_county_df = pd.DataFrame(house_county, columns=['state', 'county', 'current_votes', 'total_votes', 'percent'])
    house_county_candidate_df = pd.DataFrame(house_county_candidate,
                                             columns=['state', 'county', 'candidate', 'party', 'votes'])

    house_state_df.to_csv('data/house_state.csv', index=False)
    house_county_df.to_csv('data/house_county.csv', index=False)
    house_county_candidate_df.to_csv('data/house_county_candidate.csv', index=False)

    governors_state = []
    governors_county = []
    governors_county_candidate = []

    for state in governors.keys():
        for i, county in enumerate(governors[state].keys()):
            if i == 0:
                governors_state.append([
                    governors[state][county][0][0],
                    governors[state][county][0][1],
                    governors[state][county][0][3]
                ])
            else:
                for x, candidate in enumerate(governors[state][county][1]):
                    if x == 0:
                        governors_county.append([
                            governors_state[-1][0],
                            governors[state][county][0][0],
                            governors[state][county][0][1],
                            governors[state][county][0][2],
                            governors[state][county][0][3]
                        ])
                    else:
                        governors_county_candidate.append([
                            governors_state[-1][0],
                            governors[state][county][0][0],
                            ' '.join(candidate[:2]),
                            candidate[2],
                            candidate[4]
                        ])

    governors_state_df = pd.DataFrame(governors_state, columns=['state', 'votes', 'electoral_vote'])
    governors_county_df = pd.DataFrame(governors_county,
                                       columns=['state', 'county', 'current_votes', 'total_votes', 'percent'])
    governors_county_candidate_df = pd.DataFrame(governors_county_candidate,
                                                 columns=['state', 'county', 'candidate', 'party', 'votes'])

    governors_state_df.to_csv('data/governors_state.csv', index=False)
    governors_county_df.to_csv('data/governors_county.csv', index=False)
    governors_county_candidate_df.to_csv('data/governors_county_candidate.csv', index=False)

    api.dataset_create_version(
        "data/", f"Auto update: {last_update}", delete_old_versions=True
    )


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'interval', hours=1, timezone='America/Maceio')

    scheduler.start()
