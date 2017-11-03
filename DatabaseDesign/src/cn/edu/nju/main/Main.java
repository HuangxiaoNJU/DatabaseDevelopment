package cn.edu.nju.main;

import cn.edu.nju.dao.Init;

public class Main {

    public static void main(String[] args) {
        Init init = new Init();
        init.createTable();
    }

}
