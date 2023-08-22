#include "VirtualMemory.h"
#include "PhysicalMemory.cpp"
#include <algorithm>

#define ROOT_FRAME 0
#define CHILD_OFFSET 0
#define CURRENT_PAGE 0
#define CURRENT_DEPTH 0
uint64_t get_address (uint64_t virtualAddress);

void fill_zeros (uint64_t frame)
{
  for (uint64_t i = 0; i < PAGE_SIZE; ++i)
  {
    PMwrite ((frame * PAGE_SIZE) + i, 0);
  }
}

/**
 * if all of the entries in the frame are zero, return 1 (success), otherwise (fail) return 0.
 * @param address
 * @return 1 upon success, 0 upon failure
 */
int is_frame_of_zero (const uint64_t address)
{
  word_t current_word = 0;
  word_t sum_words = 0;
  for (int i = 0; i < PAGE_SIZE; i++)
  {
    PMread ((PAGE_SIZE * (address)) + i, &current_word);
    sum_words += current_word;
  }
  if (sum_words == 0)
  {
    return 1;
  }
  return 0;
}

void find_max (int *max, int frame_number = 0, int depth = 0)
{
  if (depth == TABLES_DEPTH)
  {
    return;
  }
  for (int i = 0; i < PAGE_SIZE; i++)
  {
    word_t data;
    PMread (frame_number * PAGE_SIZE + i, &data);
    if (data != 0)
    {
      find_max (max, data, ++depth);
      if (data > *max)
      {
        *max = data;
      }
    }
  }
  return;
}

void VMinitialize ()
{
  for (int i = 0; i < PAGE_SIZE; i++)
  {
    PMwrite (i, 0);
  }
}

int VMread (uint64_t virtualAddress, word_t *value)
{
  PMread (get_address (virtualAddress), value);
  return 0;
}

int VMwrite (uint64_t virtualAddress, word_t value)
{
  PMwrite (get_address (virtualAddress), value);
  return 0;
}

int abs (int number)
{
  if (number < 0)
  {
    return -number;
  }
  return number;
}

bool IsOkToEvict(const uint64_t* not_evict, uint64_t frame_index){
  for (int i = 0; i < TABLES_DEPTH; i++) {
    if (not_evict[i] == frame_index) {
      return false;
    }
  }
  return true;
}


int cyclic_dist (uint64_t page_swapped_in, uint64_t p)
{
  if (abs ((int) page_swapped_in - (int) p) <
      NUM_PAGES - abs ((int) page_swapped_in - (int) p))
  {
    return abs((int) page_swapped_in - (int) p);
  }
  return NUM_PAGES - abs ((int) page_swapped_in - (int) p);
}

void
recursive_dfs (uint64_t *frame_to_evict,
               int *max_frame_index,
               uint64_t
current_frame,
uint64_t desired_page,
int *max_distance,
uint64_t *page_to_evict,
uint64_t parent_frame,
int child_offset,
word_t current_page,
bool *empty_frame,
uint64_t *evicted_parent,
int *evicted_offset,
int current_depth,
uint64_t *not_evict)
{

  // in case we reach to a leaf, we save all the variables that we need in
  // order to evict it outside. at the end of the recursive call the correct
  // data will be stored in those variables
  if (current_depth == TABLES_DEPTH )
  {
    int current_distance = cyclic_dist (desired_page, current_page);
    if (current_distance > *max_distance && (!*empty_frame))
    {
      *max_distance = current_distance;
      *page_to_evict = current_page;
      *frame_to_evict = current_frame;
      *evicted_parent = parent_frame;
      *evicted_offset = child_offset;
    }
    return;
  }

  // if we found frame made of zeros only
  else if(!(*empty_frame))
  {
    if (is_frame_of_zero (current_frame) &&
        IsOkToEvict (not_evict, current_frame))
    {
      *empty_frame = true;
      *frame_to_evict = current_frame;
      *evicted_parent = parent_frame;
      *evicted_offset = child_offset;
      return;
    }
  }

  for (int offset = 0; offset < PAGE_SIZE; offset++)
  {
    word_t next_frame;

    // check if the current frame is pointing to another frame
    PMread (current_frame * PAGE_SIZE + offset, &next_frame);
    if ((next_frame != 0))
    {
      if (next_frame > *max_frame_index)
      {
        *max_frame_index = next_frame;
      }
      recursive_dfs (frame_to_evict, max_frame_index,
                     next_frame, desired_page,
                     max_distance, page_to_evict,
                     current_frame, offset,
                     (current_page << OFFSET_WIDTH) + offset, empty_frame,
                     evicted_parent, evicted_offset,
                     current_depth + 1, not_evict);
    }
  }
}

/**
 *
 * @param page virtual page that we want to store in a physical frame
 * @return physical frame the save the virtual page in it
 */
uint64_t find_frame (uint64_t page, uint64_t *not_evict)
{
  int max_frame_index = 0;
  int max_distance = 0;
  int evicted_offset = 0;
  uint64_t frame_to_evict = 0;
  uint64_t page_to_evict = 0;
  uint64_t evicted_parent;
  bool empty_frame;

  // empty frame is one that consists of zeros only,
  // evict a frame means to it unlink from its parent - fill the parent entry
  // with zero.
  // if fount an empty frame - save it in frame_to_evict. if not - the frame in
  // page_to_evict is the one to evict.
  recursive_dfs (&frame_to_evict, &max_frame_index,
                 ROOT_FRAME, page,
                 &max_distance, &page_to_evict,
                 ROOT_FRAME, CHILD_OFFSET,
                 CURRENT_PAGE, &empty_frame,
                 &evicted_parent, &evicted_offset,
                 CURRENT_DEPTH, not_evict);

  // only in the beginning of the program
  if (max_frame_index + 1 < NUM_FRAMES)
  {
    frame_to_evict = max_frame_index + 1;
//    fill_zeros (frame_to_evict);
//    return frame_to_evict;
  }
    // if found empty frame no evict needed
  else if (empty_frame)
  {
    PMwrite ((evicted_parent * PAGE_SIZE) + evicted_offset, 0);
//    return frame_to_evict;
  }
    // if evict is needed
  else
  {
    PMevict (frame_to_evict, page_to_evict);
    PMwrite ((evicted_parent * PAGE_SIZE) + evicted_offset, 0);
  }
  return frame_to_evict;
}

/**
 * return physical frame for the virtual address. evict some frames if needed.
 * @param virtualAddress desired page address in virtual memory that we want
 * to save or get its content from the physical memory
 * @return physical address
 */
uint64_t get_address (uint64_t virtualAddress)
{
  word_t address = 0;
  word_t next_address = 0;
  uint64_t not_evict[TABLES_DEPTH] = {};

  uint64_t offset = virtualAddress % PAGE_SIZE;
  uint64_t page = virtualAddress >> OFFSET_WIDTH;

  // iterates over the depth of the memory tree until the leaves which is
  // where the data is saved
  for (int i = 0; i < TABLES_DEPTH; i++)
  {
    // node == frame in the physical memory

    // cur_offset is chunk of bits determines the entry in the curren page / depth in the tree
    uint64_t cur_offset =
        (virtualAddress >> (OFFSET_WIDTH * (TABLES_DEPTH - i))) % PAGE_SIZE;

    // check if the node points to the next node or empty
    PMread (address * PAGE_SIZE + cur_offset, &next_address);

    //the node doesn't point to any node in the next level \ depth
    if (next_address == 0)
    {
      // search for the next frame, i.e. the next node in route to the leaf
      // where the data is
      next_address = find_frame (page, not_evict);

      // make the current node to point on the node we found
      fill_zeros (next_address);
      PMwrite ((address * PAGE_SIZE) + cur_offset, next_address);

    }
    not_evict[i] = next_address;
    address = next_address;
  }
  PMrestore (address, page);

  // return the physical address which is a leaf in the tree
  uint64_t physical_address = (address << OFFSET_WIDTH) + offset;
  return physical_address;
}

//vwrite -> get_address -> find_frame -> (if there is no empty frame) -> recursive_dfs




//#include "VirtualMemory.h"
//#include "PhysicalMemory.cpp"
//#include "MemoryConstants.h"
//#include <cmath>
//
//using namespace std;
//
//#define BASE 0
//
///**
// * returns the minimum of integers a and b
// * @param a the first integer
// * @param b the second integer
// * @return min(a,b)
// */
//int min(int a, int b) {
//  if (a < b) { return a; }
//  return b;
//}
//
///**
// * returns the k-th LSB bit in num
// * @param num the num we wish to get the k-th LSB bit in
// * @param k the index
// * @return the k-th LSB of num
// */
//int kBit(uint64_t num, int k) {
//  return (num >> k) & 1;
//}
//
///**
// * returns the offset (the rightmost OFFSET size LSB bits) of num
// * @param num the nmber we wish to get the bits from
// * @return num's offset
// */
//uint64_t getOffset(uint64_t num) {
//  return ((num) & ((1 << (OFFSET_WIDTH)) - 1));
//}
//
//
//void clearTable(uint64_t frameIndex) {
//  for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
//    PMwrite(frameIndex * PAGE_SIZE + i, 0);
//  }
//}
//
///**
// * checks if frame is in the array of forbidden frames
// * @param forbidden an array containing frames we cannot evict
// * @param frame a frame we wish to ask if it forbidden
// * @return true if frame is forbidden, and false otherwise
// */
//bool isForbidden(const uint64_t *forbidden, uint64_t frame) {
//  for (int i = 0; i < TABLES_DEPTH; i++) {
//    if (forbidden[i] == frame) {
//      return true;
//    }
//  }
//  return false;
//}
//
///**
// *  returns the index of the next "level" in the page table (as we learned in class - p1, p2, ect), which is
// *  done by extracting the relevant bits from the virtual address
// * @param address the adress we wish to extract the "level" index
// * @param level the level we wish to find
// * @return the index for that level of the page table
// */
//uint64_t getPINum(uint64_t address, int level) {
//  int offset = OFFSET_WIDTH * level;
//  int endOfBracket = min(OFFSET_WIDTH - 1, (int) WORD_WIDTH - offset - 1);
//  uint64_t partial = 0;
//  for (int i = endOfBracket; i >= 0; i--) {
//    partial += static_cast<uint64_t >(pow(2, i)) * kBit(address, offset + i);
//  }
//  return partial;
//}
//
///**
// * checks if the frame is empty
// * @param frameIndex the frame we wish to check
// * @return true if the frame is empty (contains only 0's) and false otherwise
// */
//bool isFrameEmpty(uint64_t frameIndex) {
//  int val;
//  for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
//    PMread(frameIndex * PAGE_SIZE + i, &val);
//    if (val) {
//      return false;
//    }
//  }
//  return true;
//}
//
///**
// * calculates the cyclic value of a page
// * @param page_swapped the pag ewe wish to swap out for
// * @param pageNum the page whom we're calculating the cyclic val
// * @return the page's value
// */
//uint64_t CyclicVal(uint64_t page_swapped, uint64_t pageNum) {
//  int temp = (int) (page_swapped - pageNum);
//  temp = abs(temp);
//  if ((NUM_PAGES - temp) > temp) {
//    return temp;
//  }
//  return NUM_PAGES - temp;
//}
//
///**
// * if the current page's cyclic value (inputted by pageNum and pageSwapped) is larger than the maximum we stored,
// * then store the new maximum
// * @param page_swapped the page that we are swapping out for
// * @param pageNum the new page for whom we are calculating it's cyclic value
// * @param maxIndexCyclicVal the page number of the previously maximum cyclic value attained
// * @param maxCyclicVal the maximum cyclic value so far in the tree
// * @param leafsFrame pageNum's frame in the physical memory
// * @param maxLeafFrame the frame of the maximal page we have seen
// * @param prev the physical address of the parent pointing to pageNum
// * @param maxPrev the physical address of the parent pointing to the maximal page we saw
// */
//void MaxCyclic(uint64_t page_swapped, uint64_t pageNum, uint64_t &maxIndexCyclicVal, uint64_t &maxCyclicVal,
//               uint64_t &leafsFrame, uint64_t &maxLeafFrame, uint64_t &prev, uint64_t &maxPrev) {
//  uint64_t pageCyclicVal = CyclicVal(page_swapped, pageNum);
//  if (pageCyclicVal > maxCyclicVal) {
//    maxCyclicVal = pageCyclicVal;
//    maxIndexCyclicVal = pageNum;
//    maxLeafFrame = leafsFrame;
//    maxPrev = prev;
//  }
//}
//
///**
// * return the page number (the last OFFSET_WIDTH LSB bits) of the virtual address
// * @param virtualAddress the address from which we will extract the page number
// * @return the page number in virtual address
// */
//uint64_t getPageNum(uint64_t virtualAddress) {
//  return virtualAddress >> OFFSET_WIDTH;
//}
//
//
///**
// * a recursive DFS function that searched the tree for a frame to that will hold a page table.
// * The function looks for frames in the following order:
// * 1) an unused frame
// * 2) an empty frame
// * 3) a leaf that can be evicted
// * @param frameNum a pointer that will hold the frame number to be returned to the main function
// * @param maxSeen the index of the maximal frame we have seen thus far
// * @param currDepth the current depth in the recursion
// * @param leafNum a counter of all of the pages (meaning leaves - which are not page tables) we have crossed
// * thus far in the recursion
// * @param maxIndexCyclicVal the index number of the page with the highest cyclic value, that we may need to evict
// * @param maxCyclicVal the value of the the page above
// * @param pageSwappedIn the number of the page we need to swap out for
// * @param maxParent the physical address of the parent pointing to the page we might need to evict
// * @param evictedFrame the index of the frame of the page we might need to evict
// * @param emptyFrame a pointer that holds the index of an empty frame
// * @param forbidden an array of frames we cannot evict
// * @param currPrev the physical address of the parent pointing to where we are in the tree
// * @param maxCycValParent the physical address of the parent pointing to the page with the highest cyclic value
// * @param emptyFrameParent the physical address of the parent pointing to the and empty frame
// */
//void findAvailableFrame(uint64_t &frameNum, uint64_t &maxSeen, int currDepth, uint64_t &leafNum,
//                        uint64_t &maxIndexCyclicVal, uint64_t &maxCyclicVal, uint64_t pageSwappedIn,
//                        uint64_t &maxParent,
//                        uint64_t &evictedFrame, uint64_t &emptyFrame, uint64_t *forbidden,
//                        uint64_t &currPrev, uint64_t &maxCycValParent, uint64_t &emptyFrameParent) {
//  /*recursively search (by DFS) a frame that we can fit the page in*/
//  //first - we'll update the maximal frame seen
//  if (maxSeen < frameNum) {
//    maxSeen = frameNum;
//  }
//  //base - we hit a leaf
//  if (currDepth == TABLES_DEPTH) {
//    //calculate the page's cyclic value, and compare it to the maximum seen
//    MaxCyclic(pageSwappedIn, leafNum, maxIndexCyclicVal, maxCyclicVal, frameNum, evictedFrame, currPrev,
//              maxCycValParent);
//    leafNum++; // increase the page index count
//    return;
//    //if we haven't seen any empty frames yet, then we can ask if the current frame is empty
//  } else if (!emptyFrame) {
//    if (isFrameEmpty(frameNum) && !isForbidden(forbidden, frameNum)) {
//      //the current frame is empty, and is not on the forbidden list - we will store it in the dedicated pointers
//      emptyFrame = frameNum;
//      emptyFrameParent = currPrev;
//    }
//  }
//  //we aren't at a leaf. that mean we can continue to search for sons
//  int val = 0;
//  for (uint64_t i = 0; i < PAGE_SIZE; i++) {
//    PMread(frameNum * PAGE_SIZE + i, &val); //read the i-th son of the current page table we are into val
//    if (val) {
//      //the i-th son is mapped to a frame - we'll head down the recursion tree
//      uint64_t old1 = frameNum, old2 = currPrev;
//      currPrev = frameNum * PAGE_SIZE + i;
//      frameNum = val;
//      findAvailableFrame(frameNum, maxSeen, currDepth + 1, leafNum,
//                         maxIndexCyclicVal, maxCyclicVal,
//                         pageSwappedIn, maxParent, evictedFrame, emptyFrame, forbidden,
//                         currPrev, maxCycValParent, emptyFrameParent);
//      frameNum = old1;
//      currPrev = old2;
//    } else {
//      //this son is an empty tree. we'll just increase the index of the leaves we would have gotten to
//      leafNum += static_cast<uint64_t >(pow(PAGE_SIZE, TABLES_DEPTH - currDepth - 1));
//    }
//  }
//}
//
///**
// * returns a frame for the requested read/write operation
// * @param virtualAddress the virtual address given
// * @return a frame that is garunteed to be either not used, empty, or fitting in cyclic value
// */
//uint64_t treeTraverse(uint64_t virtualAddress) {
//  word_t addr = 0;
//  uint64_t forbidden[TABLES_DEPTH] = {};
//  uint64_t pageNum = getPageNum(virtualAddress), prev = BASE;
//  /*
//   * we'll go down the tree, and for each page table ask if it is currently mapped to the physical memory (and not the
//   * Disk). if it is, we can continue going down, but if it isn't, then we'll call the recursive DFS function to find
//   * a frame
//   */
//  for (int currLevel = TABLES_DEPTH; currLevel > 0; currLevel--) {
//    uint64_t maxFrameUsed = 0, maxCycLeafNum = 0, maxCycVal = 0,
//        leafCounter = 0, parent = 0, evictedFrame = 0, runningParent = 0, maxParent = 0, emptyFrame = 0,
//        emptyFrameParent = 0, bracket = 0, f1 = 0;
//    //get the p-i-th section of the virtual address
//    bracket = getPINum(virtualAddress, currLevel);
//    // read the value stored in the calculated address
//    PMread(prev * PAGE_SIZE + bracket, &addr);
//    //check if addr is indeed loaded into the physical memory
//    if (addr == 0) {
//      /*addr is not loaded into memory. This means we have to find a frame to "hold" this page in.*/
//      //call the recursive function from here, and store the result in f1
//      findAvailableFrame(f1, maxFrameUsed, 0, leafCounter, maxCycLeafNum, maxCycVal, pageNum, parent,
//                         evictedFrame, emptyFrame, forbidden, runningParent, maxParent,
//                         emptyFrameParent);
//      //check what type of frame we got back from the recursive function
//      if (maxFrameUsed + 1 < NUM_FRAMES) {
//        f1 = maxFrameUsed + 1;
//      } else if (emptyFrame) {
//        PMwrite(emptyFrameParent, 0);
//        f1 = emptyFrame;
//      } else {
//        //no unused or empty frames were found. EVICTION NOTICE!!
//        PMevict(evictedFrame, maxCycLeafNum);
//        PMwrite(maxParent, 0);
//        f1 = evictedFrame;
//      }
//      clearTable(f1);
//      //f1 now holds the frame number that is available and full of zeroes
//      PMwrite((prev * PAGE_SIZE) + bracket, f1); // update the father pointing to the son's frame
//      addr = f1;
//
//    }
//    forbidden[TABLES_DEPTH - currLevel] = addr;
//    prev = addr;
//  }
//  PMrestore(addr, pageNum); //restore the page to the frame we found
//  return addr; // return the address
//}
//
//
//void VMinitialize() {
//  clearTable(0);
//}
//
//
//int VMread(uint64_t virtualAddress, word_t *value) {
//  word_t addr = treeTraverse(virtualAddress);
//  if (virtualAddress > VIRTUAL_MEMORY_SIZE) {
//    return 0;
//  }
//  PMread((addr * PAGE_SIZE) + getOffset(virtualAddress), value);
//  return 1;
//}
//
//
//int VMwrite(uint64_t virtualAddress, word_t value) {
//  word_t addr = treeTraverse(virtualAddress);
//  if (virtualAddress > VIRTUAL_MEMORY_SIZE) {
//    return 0;
//  }
//  PMwrite((addr * PAGE_SIZE) + getOffset(virtualAddress), value);
//  return 1;
//}
//
