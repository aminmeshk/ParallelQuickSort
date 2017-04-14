import java.util.Arrays;
import java.util.Random;
import mpi.*;

public class Main
{
    public static int ArraySize = 10000000;
    public static int max = 100000000;

    public static void main(String[] args)
    {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
        System.out.println("Rank: " + rank);

        if (rank == 0)
        {
            Random rand = new Random();
            int count = ArraySize;
            int[] array = new int[count];
            for (int i = 0; i < count; i++)
            {
                array[i] = rand.nextInt(max - 1) + 1;
            }
            System.out.println("Array has been generated!");
            //System.out.println(array.length);
            MPI.COMM_WORLD.Send(array, 0, (array.length / 2), MPI.INT, 1, 1);
            MPI.COMM_WORLD.Send(array, (array.length / 2), (array.length / 2), MPI.INT, 2, 2);

            int[] arr1 = new int[ArraySize / 2];
            int[] arr2 = new int[ArraySize / 2];
            MPI.COMM_WORLD.Recv(arr1, 0, arr1.length, MPI.INT, 1, 10);
            MPI.COMM_WORLD.Recv(arr2, 0, arr2.length, MPI.INT, 2, 20);
            int[] merged = merge(arr1, arr2);

            System.out.println("Merged");
            arrayToFile(merged);
        } else if (rank == 1)
        {
            int[] arr = new int[ArraySize / 2];
            //int[] arr = new int[ArraySize];
            MPI.COMM_WORLD.Recv(arr, 0, arr.length, MPI.INT, 0, 1);
            Quicksort qs = new Quicksort();
            qs.sort(arr);
            System.out.println("Sorted #1 !");
            MPI.COMM_WORLD.Send(arr, 0, arr.length, MPI.INT, 0, 10);
        } else if (rank == 2)
        {
            int[] arr = new int[ArraySize / 2];
            //int[] arr = new int[10];
            MPI.COMM_WORLD.Recv(arr, 0, arr.length, MPI.INT, 0, 2);
            Quicksort qs = new Quicksort();
            qs.sort(arr);
            System.out.println("Sorted #2 !");
            MPI.COMM_WORLD.Send(arr, 0, arr.length, MPI.INT, 0, 20);
        }
    }

    public static int[] generateArray()
    {

        Random rand = new Random();
        int count = ArraySize;
        int[] array = new int[count];
        for (int i = 0; i < count; i++)
        {
            array[i] = rand.nextInt(max - 1) + 1;
        }
        System.out.println("Array has been generated!");
        return array;

    }

    public static int[] merge(int[] a, int[] b)
    {

        int[] answer = new int[a.length + b.length];
        int i = 0, j = 0, k = 0;
        while (i < a.length && j < b.length)
        {
            if (a[i] < b[j])
            {
                answer[k] = a[i];
                i++;
            } else
            {
                answer[k] = b[j];
                j++;
            }
            k++;
        }

        while (i < a.length)
        {
            answer[k] = a[i];
            i++;
            k++;
        }

        while (j < b.length)
        {
            answer[k] = b[j];
            j++;
            k++;
        }

        return answer;
    }

    public static void arrayToFile(int[] array)
    {
        try
        {
            java.io.File file = new java.io.File("Output.txt");
            file.createNewFile();
            java.io.FileWriter wr = new java.io.FileWriter(file);
            for (int a : array)
            {
                wr.append(a + ",");
            }
            wr.flush();
            wr.close();
        } catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }
}

class Quicksort
{
    private int[] numbers;
    private int number;

    public void sort(int[] values)
    {
        // check for empty or null array
        if (values == null || values.length == 0)
        {
            return;
        }
        this.numbers = values;
        number = values.length;
        quicksort(0, number - 1);
    }

    private void quicksort(int low, int high)
    {
        int i = low, j = high;
        // Get the pivot element from the middle of the list
        int pivot = numbers[low + (high - low) / 2];

        // Divide into two lists
        while (i <= j)
        {
            // If the current value from the left list is smaller than the pivot
            // element then get the next element from the left list
            while (numbers[i] < pivot)
            {
                i++;
            }
            // If the current value from the right list is larger than the pivot
            // element then get the next element from the right list
            while (numbers[j] > pivot)
            {
                j--;
            }

            // If we have found a value in the left list which is larger than
            // the pivot element and if we have found a value in the right list
            // which is smaller than the pivot element then we exchange the
            // values.
            // As we are done we can increase i and j
            if (i <= j)
            {
                exchange(i, j);
                i++;
                j--;
            }
        }
        // Recursion
        if (low < j)
        {
            quicksort(low, j);
        }
        if (i < high)
        {
            quicksort(i, high);
        }
    }

    private void exchange(int i, int j)
    {
        int temp = numbers[i];
        numbers[i] = numbers[j];
        numbers[j] = temp;
    }
}
