import pandas as pd
import numpy as np

def main():

	#pull in unprocessed data
	raw_df=pd.read_csv('./data/raw/pitchdata.csv')
		
	output_df=pd.DataFrame()

	#open combinations.txt to retrieve data grouping rules
	with open('./data/reference/combinations.txt', 'r') as combinations:

		#discard first line header row
		foo=combinations.readline() 

		for line in combinations:
			
			args=line.split(',')
			
			#remove newline char from argument
			args[2]=args[2].replace('\n','')

			#pass arguments and create combination dataframe
			output_df=output_df.append(create_combo_dataframe(raw_df, args[0], args[1], args[2]))
			
	output_df.sort_values(['SubjectId', 'Stat', 'Split', 'Subject'], ascending=[True, True, True, True], inplace=True)

	output_df.to_csv('./data/processed/output.csv', index=False)
		
def create_combo_dataframe(raw_df, stat, subject, split):

    #get L or R split character
    split_char=split[3] 
    
    #pull hitterside or pitcherside char
    if split[5]=='H':
        
        split_filter='HitterSide'
        
    elif split[5]=='P':
        
        split_filter='PitcherSide'
        
    else:
        
        raise Exception('Unknown split type or data error')
        
	#filter dataframe on batter or pitcher handedness
    combo_df=raw_df[raw_df[split_filter]==split_char]

	#aggregate sums of category stats
    combo_df=combo_df.groupby(subject).agg({
						'PA': np.sum, 'AB': np.sum, 'H': np.sum,'2B': np.sum,
						'3B': np.sum,'HR': np.sum,'TB': np.sum,'BB': np.sum,
						'SF': np.sum,'HBP': np.sum,})

	#remove rows with fewer than 25 plate appearances
    combo_df=combo_df[combo_df['PA']>=25]
    
	#create SubjectId column
    combo_df['SubjectId']=combo_df.index
    
	#create Stat column
    combo_df['Stat']=stat
    
	#create Split column
    combo_df['Split']=split
    
	#create Subject column
    combo_df['Subject']=subject

	#Calculate AVG, OBP, SLG or OPS based on input argument
    if stat=='AVG':
        
        combo_df['Value']=(combo_df['H']
                           / combo_df['AB'])
    
    elif stat=='OBP':
        
        combo_df['Value']=((combo_df['H']
                           + combo_df['BB']
                           + combo_df['HBP'])
                           / (combo_df['AB']
                             + combo_df['BB']
                             + combo_df['HBP']
                             + combo_df['SF']))
    
    elif stat=='SLG':
        
        combo_df['Value']=(combo_df['TB']
                           / combo_df['AB'])
        
    elif stat=='OPS':
        #might be more efficient to skip this and backfill as OBP + SLG
        combo_df['Value']=(((combo_df['H']
                           + combo_df['BB']
                           + combo_df['HBP'])
                           / (combo_df['AB']
                             + combo_df['BB']
                             + combo_df['HBP']
                             + combo_df['SF']))
                               + (combo_df['TB']
                               / combo_df['AB']))

    else:
        
        raise Exception('Unknown or malformed stat request')
        
	#round data to max of 3 decimal places
    combo_df['Value']=combo_df['Value'].round(3)
    
	#return only desired columns
    return combo_df[['SubjectId', 'Stat', 'Split', 'Subject' , 'Value']]			

if __name__ == '__main__':
    main()