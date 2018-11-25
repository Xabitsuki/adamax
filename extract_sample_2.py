#!/usr/bin/env python
import subprocess 
import random

# make data directory 
subprocess.check_output(['mkdir','data'])


def get_paths(command_ar) : 

     # ls in year file 
    out_ls = subprocess.check_output(command_ar)                       
    # recover lines format 
    out_ls = out_ls.split('\n')
    # drop first and last elements
    out_ls= out_ls[1:-1]
    # get full path to movies: in output of ls, only the 
    # last element (separated by spaces) is the path 
    paths = [path.split(' ')[-1] for path in out_ls] 

    return paths

years = map(lambda x : str(x), range(1920,2019)) # check only movies from this years

for year in years: # for all years 
    
    path_to_year = '/datasets/opensubtitle/OpenSubtitles2018/xml/en/{}'.format(year)
    # get path to the movies 
    path_to_movies = get_paths(['hadoop','fs','-ls',path_to_year])
    # filter movies ids to keep only those of 7 characters  
    path_to_movies = filter(lambda path : len(path.split('/')[-1]) == 7,path_to_movies)

    if len(path_to_movies) !=0: # have movies with matching id 
        
        # make dir for year in data folder
        path_dir_year = 'data/{}'.format(year)
        subprocess.check_output(['mkdir',path_dir_year])

        # pick 2 movies 
        id1 = random.randint(0,len(path_to_movies)-1)
        if len(path_to_movies) > 1 : # at leat two movies 
            id2 = id1 
            while id2 == id1:  
        	    id2 = random.randint(0,len(path_to_movies)-1)
            path_2_movies = [path_to_movies[id1],path_to_movies[id2]]
        else:  
            path_2_movies = [path_to_movies[id1]] # only one movie 
        
        for path_1_movie in path_2_movies:
            # get name movie id 
            id_movie = path_1_movie.split('/')[-1]
            # mkdir for movie in corresponding year
            path_dir_movie = 'data/{}/{}'.format(year,id_movie) 
            subprocess.check_output(['mkdir',path_dir_movie])
            # get paths to substitles 
            path_to_subs = get_paths(['hadoop','fs','-ls', path_1_movie])

            for path_to_sub in path_to_subs:
                filename = path_to_sub.split('/')[-1]
                path_file = '{}/{}'.format(path_dir_movie,filename)
                # copy files into personnal directory 
                subprocess.check_output(['hadoop','fs','-get',path_to_sub,path_file])
        print('finish moving for year : {}'.format(year))

    else : print('No movies with id of 7 chars in {}'.format(year))
