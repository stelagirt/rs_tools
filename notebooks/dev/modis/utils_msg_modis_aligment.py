import pyproj
from satpy.scene import Scene
from pathlib import Path

def check_overlap(modis_im, msg_im):
    lonmax_modis, lonmin_modis = modis_im.x.max().item(), modis_im.x.min().item()
    latmax_modis, latmin_modis = modis_im.y.max().item(), modis_im.y.min().item()
    print(lonmin_modis, lonmax_modis, latmin_modis, latmax_modis)
    
    lonmax_msg, lonmin_msg = msg_im.x.max().item(), msg_im.x.min().item()
    latmax_msg, latmin_msg = msg_im.y.max().item(), msg_im.y.min().item()
    print(lonmin_msg, lonmax_msg, latmin_msg, latmax_msg)
    
    #check if they overlap and add a column to df_matches 
    if not (lonmin_modis > lonmax_msg or lonmax_modis < lonmin_msg or latmin_modis > latmax_msg or latmax_modis < latmin_msg):
        #print("They overlap")
        return True
    else:
        #print("No overlap")
        return False
    
def clip_and_align(msg_reprojected, modis_reprojected):
    modis_reprojected = modis_reprojected.fillna(0)
    modis_interp = modis_reprojected.interp(x = msg_reprojected.x, y = msg_reprojected.y, method = 'nearest')
    valid3d = modis_interp.notnull()                      # True where not-NaN
    mask2d = valid3d.isel(band=0, drop=True).astype(bool)   # (y, x)
    mask3d = mask2d.expand_dims(band=msg_reprojected['band']) 
    msg_clipped = msg_reprojected.Rad.where(mask3d, drop=True)
    modis_clipped = modis_interp.where(valid3d, drop=True) 
    return(msg_clipped, modis_clipped)

def reproject_msg_image(image, crs_wkt, ref_nat_file = '/mnt/outputs/L1b/MSG4-SEVI-MSG15-0100-NA-20210601232742.451000000Z-NA.nat'):
    if crs_wkt is None:
        scn = Scene(reader="seviri_l1b_native", filenames=[ref_nat_file])
        datasets = scn.available_dataset_names()
        scn.load(datasets[1:], generate=False)
        dataset = scn['IR_016']
        crs_wkt = dataset.attrs['area'].crs_wkt

    # Define the source CRS from the WKT string
    source_crs = pyproj.CRS(crs_wkt)
    target_crs = 'EPSG:4326' # Global lat-lon coordinate system
    # copy dataset
    new_dataset = image.copy(deep=True)

    # assign CRS to dataarray
    new_dataset = new_dataset.rio.write_crs(source_crs, inplace=False)
    try:
        new_dataset = new_dataset.drop('acq_time') # Drop acq_time coordinate, which was causing issues # NOTE: Not sure why this is necessary?
    except:
        i = 2
    new_dataset = new_dataset.rio.reproject(target_crs)
    return new_dataset

def parse_af_dates_from_file(file):
    timestamp = Path(file).name.split("_")[0]
    return timestamp

def check_overlap(modis_im, msg_im):
    lonmax_modis, lonmin_modis = modis_im.x.max().item(), modis_im.x.min().item()
    latmax_modis, latmin_modis = modis_im.y.max().item(), modis_im.y.min().item()
    print(lonmin_modis, lonmax_modis, latmin_modis, latmax_modis)
    
    lonmax_msg, lonmin_msg = msg_im.x.max().item(), msg_im.x.min().item()
    latmax_msg, latmin_msg = msg_im.y.max().item(), msg_im.y.min().item()
    print(lonmin_msg, lonmax_msg, latmin_msg, latmax_msg)
    
    #check if they overlap and add a column to df_matches 
    if not (lonmin_modis > lonmax_msg or lonmax_modis < lonmin_msg or latmin_modis > latmax_msg or latmax_modis < latmin_msg):
        #print("They overlap")
        return True
    else:
        #print("No overlap")
        return False