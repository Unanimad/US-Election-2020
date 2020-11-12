import pandas as pd
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

from kaggle import KaggleApi


def main():
    summary = requests.get('https://graphics.thomsonreuters.com/2020-US-elex/20201103/20201103-summary.json').json()
    county = requests.get('https://graphics.thomsonreuters.com/2020-US-elex/20201103/20201103-county.json').json()

    last_update = county['timestamp']

    president = county['P']
    senate = [county['S'], county['S2']]
    house = summary['H']
    governors = county['G']

    president_state = []
    president_county = []
    president_county_candidate = []

    for state in president.keys():
        for i, county in enumerate(president[state].values()):
            if i == 0:
                president_state.append(county[0][:2])

            else:
                for x, county_info in enumerate(county):
                    if x == 0:
                        president_county.append([
                            president_state[-1][0],
                            county_info[0],
                            county_info[1],
                            county_info[2],
                            county_info[3]
                        ])
                    else:
                        for c, candidate in enumerate(county_info):
                            won = False
                            if c == 0:
                                won = True
                            president_county_candidate.append([
                                president_state[-1][0],
                                president_county[-1][1],
                                ' '.join(candidate[:2]),
                                candidate[2],
                                candidate[4],
                                won
                            ])

    president_state_df = pd.DataFrame(president_state, columns=['state', 'total_votes'])
    president_county_df = pd.DataFrame(president_county,
                                       columns=['state', 'county', 'current_votes', 'total_votes', 'percent'])
    president_county_candidate_df = pd.DataFrame(president_county_candidate,
                                                 columns=['state', 'county', 'candidate', 'party', 'total_votes',
                                                          'won'])

    president_state_df.to_csv('data/president_state.csv', index=False)
    president_county_df.to_csv('data/president_county.csv', index=False)
    president_county_candidate_df.to_csv('data/president_county_candidate.csv', index=False)

    senate_state = []
    senate_county = []
    senate_county_candidate = []

    for s in senate:
        for state in s.keys():
            for i, county in enumerate(s[state].keys()):
                if i == 0:
                    senate_state.append([
                        s[state][county][0][0],
                        s[state][county][0][1]
                    ])
                else:
                    for x, candidate in enumerate(s[state][county][1]):
                        if x == 0:
                            senate_county.append([
                                senate_state[-1][0],
                                s[state][county][0][0],
                                s[state][county][0][1],
                                s[state][county][0][2],
                                s[state][county][0][3]
                            ])
                        else:
                            senate_county_candidate.append([
                                senate_state[-1][0],
                                s[state][county][0][0],
                                ' '.join(candidate[:2]),
                                candidate[2],
                                candidate[4]
                            ])

    senate_state_df = pd.DataFrame(senate_state, columns=['state', 'total_votes'])
    senate_county_df = pd.DataFrame(senate_county,
                                    columns=['state', 'county', 'current_votes', 'total_votes', 'percent'])
    senate_county_candidate_df = pd.DataFrame(senate_county_candidate,
                                              columns=['state', 'county', 'candidate', 'party', 'total_votes'])

    senate_state_df.to_csv('data/senate_state.csv', index=False)
    senate_county_df.to_csv('data/senate_county.csv', index=False)
    senate_county_candidate_df.to_csv('data/senate_county_candidate.csv', index=False)

    house_state = []
    house_county_candidate = []

    for state in house.keys():
        for i, county in enumerate(house[state]):
            if i == 0:
                house_state.append(county[:4])
            else:
                for x, candidate in enumerate(house[state][i]):
                    won = False
                    if x == 0:
                        won = True

                    house_county_candidate.append([
                        house_state[-1][0],
                        ' '.join(candidate[:2]),
                        candidate[2],
                        candidate[4],
                        won
                    ])

    house_state_df = pd.DataFrame(house_state, columns=['district', 'current_votes', 'total_votes', 'percent'])
    house_county_candidate_df = pd.DataFrame(house_county_candidate,
                                             columns=['district', 'candidate', 'party', 'total_votes', 'won'])

    house_state_df.to_csv('data/house_state.csv', index=False)
    house_county_candidate_df.to_csv('data/house_candidate.csv', index=False)

    governors_state = []
    governors_county = []
    governors_county_candidate = []

    for state in governors.keys():
        for i, county in enumerate(governors[state].values()):
            if i == 0:
                governors_state.append(county[0][:2])

            else:
                for x, county_info in enumerate(county):
                    if x == 0:
                        governors_county.append([
                            governors_state[-1][0],
                            county_info[0],
                            county_info[1],
                            county_info[2],
                            county_info[3]
                        ])
                    else:
                        for c, candidate in enumerate(county_info):
                            won = False
                            if c == 0:
                                won = True
                            governors_county_candidate.append([
                                governors_state[-1][0],
                                governors_county[-1][1],
                                ' '.join(candidate[:2]),
                                candidate[2],
                                candidate[4],
                                won
                            ])

    governors_state_df = pd.DataFrame(governors_state, columns=['state', 'votes'])
    governors_county_df = pd.DataFrame(governors_county,
                                       columns=['state', 'county', 'current_votes', 'total_votes', 'percent'])
    governors_county_candidate_df = pd.DataFrame(governors_county_candidate,
                                                 columns=['state', 'county', 'candidate', 'party', 'votes', 'won'])

    governors_state_df.to_csv('data/governors_state.csv', index=False)
    governors_county_df.to_csv('data/governors_county.csv', index=False)
    governors_county_candidate_df.to_csv('data/governors_county_candidate.csv', index=False)

    api = KaggleApi()
    api.authenticate()

    api.dataset_create_version(
        "data/", f"Auto update: {last_update}", delete_old_versions=True
    )


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'interval', days=1, timezone='America/Maceio')

    scheduler.start()
