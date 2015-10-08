package TestDS;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.thrift.TException;
import org.hypertable.thrift.*;
import org.hypertable.thriftgen.*;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

public class TestDSClient extends DB implements HyperTableConstants {

    private ThriftClient client;
    long namespace;

    /* init */
    public boolean init() throws DBException {
        try {
            client = ThriftClient.create(HOST_IP, PORT_NUM, 16000000, true, 400*1024*1024);
            if (!client.namespace_exists(HTNAMESPACE)) {
                client.namespace_create(HTNAMESPACE);
            }
            namespace = client.namespace_open("bgclient");
        } catch (Exception e) {
            e.printStackTrace(System.out);
            return false;
        }
        return true;
    }

    /* Create schema for tables */

    public void createSchema(Properties props) {
        createTable(MemberTable, MemberCF);
        createTable(PendingFriendsTable, PendingFriendsTableCF);
        createTable(ConfirmedFriendsTable, ConfirmedFriendsTableCF);
        createTable(ResourceTable, ResourceCF);
        createTable(ManipulationTable, ManipulationCF);
    }

    public void createTable(String tableName, String[] columnFamily) {
        try {
            /* drop existing tables */
            boolean if_exists = true;
            client.table_drop(namespace, tableName, if_exists);

            Schema schema = new Schema();

            Map<String, ColumnFamilySpec> column_families = new HashMap<String, ColumnFamilySpec>();

            ColumnFamilySpec cf;
            for (String column : columnFamily) {
                cf = new ColumnFamilySpec();
                cf.setName(column);
                column_families.put(column, cf);
            }

            schema.setColumn_families(column_families);
            client.table_create(namespace, tableName, schema);

        } catch (ClientException e) {
            System.out.println(e.message);
            System.exit(1);
        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values,
            boolean insertImage) {
        // TODO Auto-generated method stub
        /* Insert into table */
        try {
            long mutator = client.mutator_open(namespace, entitySet, 0, 0);
            List<Cell> cells = new ArrayList<Cell>();
            Key key = null;
            Cell cell = null;

            for (HashMap.Entry<String, ByteIterator> entry : values.entrySet()) {
                key = new Key(entityPK, entry.getKey(), null, KeyFlag.INSERT);
                cell = new Cell(key);
                cell.setValue(entry.getValue().toArray());
                cells.add(cell);
            }

            client.mutator_set_cells(mutator, cells);
            client.mutator_flush(mutator);
            cells.clear();
            client.mutator_close(mutator);
        } catch (ClientException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return -1;
        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
            boolean insertImage, boolean testMode) {
        // TODO Auto-generated method stub
        int pendingFriends = 0;
        int confirmedFriends = 0;
        int resourceCnt = 0;
        HqlResult queryRes;
        try {
            result.put("userid", new ObjectByteIterator(String.valueOf(profileOwnerID).getBytes("UTF-8")));
            /* Get all unique users from user table */
            HqlResultAsArrays result_as_arrays = client.hql_query_as_arrays(namespace,
                    "SELECT * from users where row='" + profileOwnerID + "'");
            for (List<?> cell_as_array : result_as_arrays.cells) {

                if (!insertImage
                        && (cell_as_array.get(1).toString().equalsIgnoreCase("pic") || cell_as_array.get(1).toString().equalsIgnoreCase("tpic"))) {
                    continue;
                } else {
                    result.put(cell_as_array.get(1).toString(),
                            new ObjectByteIterator(cell_as_array.get(3).toString().getBytes("UTF-8")));
                }
            }

            /* get resource cnt */
            queryRes = client.hql_query(namespace,
                    "SELECT * from " + ResourceTable + " where walluserid='" + profileOwnerID + "'");
            resourceCnt += queryRes.cells.size();

            /* get pending friends */
            if (requesterID == profileOwnerID) {
                queryRes = client.hql_query(namespace,
                        "SELECT * from " + PendingFriendsTable + " where row='" + profileOwnerID + "'");
                pendingFriends += queryRes.cells.size();
                result.put("pendingcount", new ObjectByteIterator(String.valueOf(pendingFriends).getBytes("UTF-8")));
            }
            /* get confirmed friends */
            queryRes = client.hql_query(namespace,
                    "SELECT * from " + ConfirmedFriendsTable + " where row='" + profileOwnerID + "'");
            confirmedFriends += queryRes.cells.size();

            /* Populate hash map */
            result.put("resourcecount", new ObjectByteIterator(String.valueOf(resourceCnt).getBytes("UTF-8")));

            result.put("friendcount", new ObjectByteIterator(String.valueOf(pendingFriends).getBytes("UTF-8")));

        } catch (Exception e) {
            e.printStackTrace(System.out);
            return -1;
        }

        return 0;
    }


    public int acceptFriend(int inviterID, int inviteeID) {
        // TODO Auto-generated method stub]
        /*
         * long mutator; /* try { mutator = client.mutator_open(namespace,
         * PendingFriendsTable, 0, 0);
         * 
         * List<Cell> cells = new ArrayList<Cell>(); Key key = null; Cell cell =
         * null;
         * 
         * key = new Key(inviterID+"",PendingFriendsTableCF[1], null,
         * KeyFlag.INSERT); cell = new Cell(key);
         * cell.setValue((inviteeID+"").getBytes("UTF-8")); cells.add(cell);
         * 
         * client.mutator_set_cells(mutator, cells);
         * client.mutator_flush(mutator); cells.clear();
         * 
         * } catch (TException e) { // TODO Auto-generated catch block
         * e.printStackTrace(); return -1; } catch (UnsupportedEncodingException
         * e) { // TODO Auto-generated catch block e.printStackTrace(); return
         * -1; }
         */
        return 0;

    }

    public int inviteFriend(int inviterID, int inviteeID) {
        // TODO Auto-generated method stub
        long mutator;
        try {
            mutator = client.mutator_open(namespace, PendingFriendsTable, 0, 0);

            List<Cell> cells = new ArrayList<Cell>();
            Key key = null;
            Cell cell = null;

            key = new Key(inviterID + "", PendingFriendsTableCF[1], null, KeyFlag.INSERT);
            cell = new Cell(key);
            cell.setValue((inviteeID + "").getBytes("UTF-8"));
            cells.add(cell);

            client.mutator_set_cells(mutator, cells);
            client.mutator_flush(mutator);
            cells.clear();

        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return -1;
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return -1;
        }
        return 0;
    }


    public HashMap<String, String> getInitialStats() {
        HashMap<String, String> result = new HashMap<String, String>();
        int userCnt = 0;
        int pendingFriends = 0;
        int confirmedFriends = 0;
        int resourceCnt = 0;
        HashSet<String> users = new HashSet<String>();
        HqlResult queryRes;
        try {
            /* Get all unique users from user table */
            HqlResultAsArrays result_as_arrays = client.hql_query_as_arrays(namespace, "SELECT * from users");
            for (List<?> cell_as_array : result_as_arrays.cells)
                users.add(cell_as_array.get(0).toString());
            userCnt = users.size();

            /* Iterate all the users */
            for (String user : users) {
                /* get resource cnt */
                queryRes = client.hql_query(namespace,
                        "SELECT * from " + ResourceTable + " where creatorid='" + user + "'");
                resourceCnt += queryRes.cells.size();

                /* get pending friends */
                queryRes = client.hql_query(namespace,
                        "SELECT * from " + PendingFriendsTable + " where row='" + user + "'");
                pendingFriends += queryRes.cells.size();

                /* get confirmed friends */
                queryRes = client.hql_query(namespace,
                        "SELECT * from " + ConfirmedFriendsTable + " where row='" + user + "'");
                confirmedFriends += queryRes.cells.size();
            }

            /* Calculate average count */
            resourceCnt = resourceCnt > 0 ? resourceCnt / userCnt : 0;
            pendingFriends = pendingFriends > 0 ? pendingFriends / userCnt : 0;
            confirmedFriends = confirmedFriends > 0 ? confirmedFriends / userCnt : 0;

            /* Populate hash map */
            result.put("usercount", String.valueOf(userCnt));
            result.put("resourcesperuser", String.valueOf(resourceCnt));
            result.put("avgfriendsperuser", String.valueOf(confirmedFriends));
            result.put("avgpendingperuser", String.valueOf(pendingFriends));
            return result;

        } catch (Exception e) {
            e.printStackTrace(System.out);
            return null;
        }
    }

    public int CreateFriendship(int friendid1, int friendid2) {
        // TODO Auto-generated method stub
        long mutator;
        try {
            mutator = client.mutator_open(namespace, ConfirmedFriendsTable, 0, 0);

            List<Cell> cells = new ArrayList<Cell>();
            Key key = null;
            Cell cell = null;

            key = new Key(friendid1 + "", ConfirmedFriendsTableCF[1], null, KeyFlag.INSERT);
            cell = new Cell(key);
            cell.setValue((friendid2 + "").getBytes("UTF-8"));
            cells.add(cell);

            key = new Key(friendid2 + "", ConfirmedFriendsTableCF[1], null, KeyFlag.INSERT);
            cell = new Cell(key);
            cell.setValue((friendid1 + "").getBytes("UTF-8"));
            cells.add(cell);

            client.mutator_set_cells(mutator, cells);
            client.mutator_flush(mutator);
            cells.clear();

        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return -1;
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return -1;
        }
        return 0;

    }



}
