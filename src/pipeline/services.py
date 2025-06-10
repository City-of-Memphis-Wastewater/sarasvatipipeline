from src.pipeline.helpers import load_toml

from src.pipeline.eds_point import Point

def populate_multiple_generic_points_from_filelist(filelist):
    # This expect a toml file which identified one point
    # Goal: expect a CSV file which identifies several points, one point per row
    for f in filelist:
        dic = load_toml(f)
        populate_generic_point_from_dict(dic)

def populate_multiple_generic_points_from_dicts(loaded_dicts):
    for dic in loaded_dicts:
        populate_generic_point_from_dict(dic)

def populate_generic_point_from_dict(dic):
    Point().populate_eds_characteristics(
        ip_address=dic["ip_address"],
        iess=dic["iess"],
        sid=dic["sid"],
        zd=dic["zd"]
    ).populate_manual_characteristics(
        shortdesc=dic["shortdesc"]
    ).populate_rjn_characteristics(
        rjn_siteid=dic["rjn_siteid"],
        rjn_entityid=dic["rjn_entityid"],
        rjn_name=dic["rjn_name"]
    )

if __name__ == "__main__":
    # Set up project manager
    from src.pipeline.projectmanager import ProjectManager
    from src.pipeline.queriesmanager import QueriesManager
    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    queries_manager = QueriesManager(project_manager)
    query_file_paths = queries_manager.get_query_file_paths_list() # no args will use whatever is identified in default-queries.toml
    populate_multiple_generic_points_from_filelist(filelist = query_file_paths)