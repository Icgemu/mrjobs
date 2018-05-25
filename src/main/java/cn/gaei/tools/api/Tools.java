package cn.gaei.tools.api;

public class Tools {

    private final static  double EARTH_RADIUS = 6378.137;//地球半径

    /**
     * @param d
     * @return
     */
    private static double rad(double d)
    {
        return d * Math.PI / 180.0;
    }

    /** 计算两点之间的大圆距离
     * @param lat1 第一个点纬度
     * @param lng1 第一个点经度
     * @param lat2 第二个点纬度
     * @param lng2 第二个点经度
     * @return 两点间大圆距离
     */
    public static double distance(double lat1, double lng1, double lat2, double lng2)
    {
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lng1) - rad(lng2);

        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2) +
                Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2)));
        s = s * EARTH_RADIUS*1000;
        return s;
    }
}
