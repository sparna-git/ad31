from src.tools import read_yaml as ry
from src.dataset import metadata
from src.notices_df import relation
from src.notices import readNotices

if __name__ == "__main__":
    
    # Read Yaml config 
    configuration = ry("config.yml")
    
    # Create Datamarts
    datamarts = metadata(configuration)

    # Read Notices
    s = relation(datamarts)
    s.read_notices()


    # Get of resource dataset
    #graph = readNotices(datamarts)
    #graph.generate_json_ld()