package com.ncut.wc.utils;

public class GeoHash {
    private static final String BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";

    public static String encode(double latitude, double longitude, int precision) {
        boolean even = true;
        int bit = 0, ch = 0;
        double[] lat = {-90.0, 90.0};
        double[] lon = {-180.0, 180.0};
        StringBuilder geohash = new StringBuilder();

        while (geohash.length() < precision) {
            double mid;
            if (even) {
                mid = (lon[0] + lon[1]) / 2;
                if (longitude >= mid) {
                    ch |= 1 << (4 - bit);
                    lon[0] = mid;
                } else {
                    lon[1] = mid;
                }
            } else {
                mid = (lat[0] + lat[1]) / 2;
                if (latitude >= mid) {
                    ch |= 1 << (4 - bit);
                    lat[0] = mid;
                } else {
                    lat[1] = mid;
                }
            }
            even = !even;
            if (++bit == 5) {
                geohash.append(BASE32.charAt(ch));
                bit = 0;
                ch = 0;
            }
        }

        return geohash.toString();
    }
}
