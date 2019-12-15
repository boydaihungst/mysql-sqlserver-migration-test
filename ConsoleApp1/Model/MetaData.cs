
namespace ConsoleApp1.Model
{
    public class MetaData
    {

        public string Name { get; set; }
        public int Column { get; set; }
        public int Row { get; set; }
        protected MetaData()
        {
        }

        public MetaData(string name,int column = 0, int row = 0)
        {
            Name = name;
            Column = column;
            Row = row;
        }

    }
}
