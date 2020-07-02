package hdfs

import (
	"os"

	"github.com/golang/protobuf/proto"
	hdfs "github.com/zxexz/hdfs/v2/internal/protocol/hadoop_hdfs"
)

// Rename renames (moves) a file.
func (c *Client) Rename(oldpath, newpath string) error {
	_, err := c.getFileInfo(newpath)
	err = interpretException(err)
	if err != nil && !os.IsNotExist(err) {
		return &os.PathError{"rename", newpath, err}
	}

	req := &hdfs.Rename2RequestProto{
		Src:           proto.String(oldpath),
		Dst:           proto.String(newpath),
		OverwriteDest: proto.Bool(true),
	}
	resp := &hdfs.Rename2ResponseProto{}

	err = c.namenode.Execute("rename2", req, resp)
	if err != nil {
		return &os.PathError{"rename", oldpath, interpretException(err)}
	}

	return nil
}
