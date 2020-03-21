package com.sinosoft.javas;

import java.util.LinkedList;

/**
 * 字典树
 */
public class TrieTree {
    public int flag;
    public String ch;
    public int leng;
    public LinkedList<TrieTree> next;
}
class save{
    public TrieTree root = new TrieTree();
    public String[] ss = new String[100];
    public void BuildTrie(String str){
        String[] s=str.split("&&");
        int i,j,len = s.length;
        TrieTree t = root;
        for(i=0;i<len;i++){
            for(j=0;j<t.leng;j++){
                if(t.next.get(j).ch == s[i])
                    break;
            }
            if(j < t.leng)
                t = t.next.get(j);
            else{
                TrieTree h = new TrieTree();
                h.ch = s[i];
                h.flag = 0;
                if(t.leng > 0)
                    t.next = new LinkedList<TrieTree>();
                t.next.add(h);
                t.leng++;
//                t = h;
//                t.leng = 0;
            }
        }
        t.flag=1;
    };
    public void Show(TrieTree t,int index){
        int i;
        for(i=0;i<t.leng;i++){
            ss[index]=t.next.get(i).ch;
            if(t.next.get(i).flag==1){
                System.out.println(new String(ss[i]));
            }
            Show(t.next.get(i),index+1);
        }
    }
    public void set(String name){
        root.leng = 0;
        BuildTrie(name);
        Show(root,0);
    }
}
class MainClass2{
    public static void main(String[] args){
//        String name = "张东宝&&卢东青";
        String name2 = "张东宝&&张则";
        String name3 = "张东宝&&卢东青&&张则";
        String name4 = "张东宝&&卢东青&&gg";
        save to = new save();
        to.set(name4);
        to.set(name2);
        to.set(name3);
        to.Show(to.root,1);
    }
}