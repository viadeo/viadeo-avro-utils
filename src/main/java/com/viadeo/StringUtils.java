package com.viadeo;




public class StringUtils {

    public static String mkString(String[] strs, String sep, String format) {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for(String str:strs){
            if(isFirst) {isFirst=false;} else {sb.append(sep);}
            sb.append(String.format(format,str));
        }
        return sb.toString();
    }

    public static String mkString(String[] strs, String sep) {
        return mkString(strs,sep,"%s");
    }

    private static int minimum(int a, int b, int c) {
        return Math.min(Math.min(a, b), c);
    }

    public static int computeLevenshteinDistance(String str1,String str2) {
        int[][] distance = new int[str1.length() + 1][str2.length() + 1];

        for (int i = 0; i <= str1.length(); i++)
            distance[i][0] = i;
        for (int j = 1; j <= str2.length(); j++)
            distance[0][j] = j;

        for (int i = 1; i <= str1.length(); i++)
            for (int j = 1; j <= str2.length(); j++)
                distance[i][j] = minimum(
                        distance[i - 1][j] + 1,
                        distance[i][j - 1] + 1,
                        distance[i - 1][j - 1]+ ((str1.charAt(i - 1) == str2.charAt(j - 1)) ? 0 : 1));

        return distance[str1.length()][str2.length()];
    }

    public static int indexOfClosestElement(String name, String[] diffins) {
        int minleven = StringUtils.computeLevenshteinDistance(name, diffins[0]);
        int indexleven = 0;
        for(int i = 0; i < diffins.length; i ++) {
            int d = StringUtils.computeLevenshteinDistance(name, diffins[i]);
            if(d < minleven) {
                minleven = d;
                indexleven = i;
            }
        }
        return indexleven;
    }
}
