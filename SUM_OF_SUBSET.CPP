#include <bits/stdc++.h>
using namespace std;

void PrintSubsetSum(int i, int n, int set[], int targetSum,
                    vector<int>& subset)
{

    if (targetSum == 0) {

        for (int i = 0; i < subset.size(); i++) {
            cout << subset[i] << " ";
        }
        cout << endl;
        return;
    }

    if (i == n) {

        return;
    }

    PrintSubsetSum(i + 1, n, set, targetSum, subset);

    if (set[i] <= targetSum) {

        subset.push_back(set[i]);

        PrintSubsetSum(i + 1, n, set, targetSum - set[i],
                       subset);

        subset.pop_back();
    }
}
int main()
{
    int set[] = { 3, 34, 4, 12, 5, 2 };
    int sum = 9;
    int n = sizeof(set) / sizeof(set[0]);
    vector<int> subset;
    PrintSubsetSum(0, n, set, sum, subset);
    return 0;
}
