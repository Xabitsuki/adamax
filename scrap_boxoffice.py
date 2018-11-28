#!/usr/bin/env python 
import requests  
from bs4 import BeautifulSoup as BfS

def parser_BO(dollar_string): 
	""" fucntions that gets the line "$value\n", removes
	the $ and the \n, the comas and returns the value in 
	to float type """

	# remove the $ 
	dollar_string = dollar_string.split('$')[1]
	# remove the \n 
	dollar_string = dollar_string.split('\n')[0]
	# remove the comas
	dollar_string = dollar_string.replace(',', '')
	# convert to float and return 
	return float(dollar_string)


def scrap_box_office(ttconst, flag_print=False): 
	""" functions the scracpes 3 values present on the 
	imdb pages : Opening weekend USA, Gross USA and 
	Cumulative Worlwide Gross"""

	URL = 'https://www.imdb.com/title/' + ttconst
	r = requests.get(URL)

	# define return vars 
	bo_wk_usa    = None
	bo_gross_usa = None 
	bo_cum_world = None

	# check id code status is ok : 200 => ok 
	if r.status_code == 200: 
		
		soup = BfS(r.content, 'html.parser')
		
		# select the detail section containing the box office info 
		details = soup.find('div', {"id": "titleDetails"})

		# target the following entries 
		str_wk_usa = 'Opening Weekend USA:'
		str_gross_usa = 'Gross USA:'
		str_cum_world = 'Cumulative Worldwide Gross:'

		div_h4s = details.find_all('h4', class_ = 'inline')

		for div_h4 in div_h4s:
			
			if div_h4.string == str_wk_usa : 
				bo_wk_usa = parser_BO(div_h4.next_sibling)
				if flag_print : print('{} : ok'.format(str_wk_usa))
			
			if div_h4.string == str_gross_usa : 
				bo_gross_usa = parser_BO(div_h4.next_sibling)
				if flag_print : print('{} : ok'.format(str_gross_usa))
			
			if div_h4.string == str_cum_world : 
				bo_cum_world = parser_BO(div_h4.next_sibling)
				if flag_print : print('{} : ok'.format(str_cum_world))

		return bo_wk_usa, bo_gross_usa, bo_cum_world

	# could not reach the request page  
	else : 

		if flag_print : print("Page not found for {}".format(ttconst))
		return bo_wk_usa, bo_gross_usa, bo_cum_world

# Testing the function 
if __name__ == '__main__':

	id_ = 1675434
	ttconst = 'tt{}'.format(id_)

	bo_wk_usa, bo_gross_usa, bo_cum_world = scrap_box_office(ttconst=ttconst,
															 flag_print=True) 
	str_wk_usa = 'Opening Weekend USA:'
	str_gross_usa = 'Gross USA:'
	str_cum_world = 'Cumulative Worldwide Gross:'

	print('\n\n')
	print('{} : {}'.format(str_wk_usa   ,bo_wk_usa))
	print('{} : {}'.format(str_gross_usa,bo_gross_usa))
	print('{} : {}'.format(str_cum_world,bo_cum_world)) 
