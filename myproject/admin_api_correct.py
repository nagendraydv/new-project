
import falcon,sys
sys.path.append("/home/nagendra/Downloads/Swagger/apis/")
from falcon_multipart.middleware import MultipartMiddleware
from ml_get_list_all_document import getDocListResource as MlGetDocListResource  #done
from ml_add_company_group import addCompanyGroupResource as MlAddCompanyGroupResource  #Done
from ml_get_company_details import companyDetailsResource as MlGetCompanyDetailsResource  #Done
from ml_add_city_mapping import cityMappingResource as MlCityMappingResource   
from ml_get_company_list_group import companyListAndGroupResource as MlCompanyListAndGroupResource
from ml_add_company import addCompanyResource as MlAddCompanyResource
from ml_get_list_all_cities import getCityListResource as MlGetCityListResource
from ml_get_available_document import getDocAvailableResource as MlGetDocAvailableResource
app=falcon.API(middleware=[MultipartMiddleware()])
mlGetDocList=MlGetDocListResource()
mlAddCompanyGroup=MlAddCompanyGroupResource()
mlGetCompanyDetails=MlGetCompanyDetailsResource()
mlCityMapping=MlCityMappingResource()
mlCompanyListGroup=MlCompanyListAndGroupResource()
mlAddCompany=MlAddCompanyResource()
mlGetCityList=MlGetCityListResource()
mlGetDocAvailable=MlGetDocAvailableResource()
app.add_route('/mlGetDocList',mlGetDocList)
app.add_route('/mlAddCompanyGroup',mlAddCompanyGroup)
app.add_route('/mlGetCompanyDetails',mlGetCompanyDetails)
app.add_route('/mlCityMapping',mlCityMapping)
app.add_route('/mlCompanyListGroup',mlCompanyListGroup)
app.add_route('/mlAddCompany',mlAddCompany)
app.add_route('/mlGetCityList',mlGetCityList)
app.add_route('/mlGetDocAvailable',mlGetDocAvailable)

